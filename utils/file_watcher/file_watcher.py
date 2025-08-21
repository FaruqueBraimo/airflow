"""
File watcher module for monitoring folder-based JSON input files.
"""
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Generator
from datetime import datetime
import shutil
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logger = logging.getLogger(__name__)


class FileWatcherConfig:
    """Configuration for file watcher."""
    
    def __init__(
        self,
        input_dir: str = "input",
        archive_dir: str = "archive", 
        error_dir: str = "error",
        file_pattern: str = "*.json",
        process_existing: bool = True,
        batch_size: int = 50,
        polling_interval: int = 5
    ):
        self.input_dir = Path(input_dir)
        self.archive_dir = Path(archive_dir)
        self.error_dir = Path(error_dir)
        self.file_pattern = file_pattern
        self.process_existing = process_existing
        self.batch_size = batch_size
        self.polling_interval = polling_interval
        
        # Ensure directories exist
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.error_dir.mkdir(parents=True, exist_ok=True)


class FinancialStatementFileHandler(FileSystemEventHandler):
    """Handler for file system events."""
    
    def __init__(self, file_watcher):
        self.file_watcher = file_watcher
        
    def on_created(self, event):
        """Handle file creation events."""
        if not event.is_directory and event.src_path.endswith('.json'):
            logger.info(f"New file detected: {event.src_path}")
            # Add small delay to ensure file is fully written
            time.sleep(1)
            self.file_watcher.add_file_to_queue(Path(event.src_path))
    
    def on_moved(self, event):
        """Handle file move events."""
        if not event.is_directory and event.dest_path.endswith('.json'):
            logger.info(f"File moved to watch directory: {event.dest_path}")
            time.sleep(1)
            self.file_watcher.add_file_to_queue(Path(event.dest_path))


class FinancialStatementFileWatcher:
    """
    File watcher for processing financial statement JSON files.
    Provides the same interface as KafkaConsumer for seamless integration.
    """
    
    def __init__(self, config: Optional[FileWatcherConfig] = None):
        """Initialize the file watcher."""
        self.config = config or FileWatcherConfig()
        self.file_queue = []
        self.observer = None
        self.is_connected = False
        
        logger.info(f"File watcher initialized for directory: {self.config.input_dir}")
    
    def connect(self) -> bool:
        """
        Start watching the input directory.
        
        Returns:
            bool: True if watching started successfully
        """
        try:
            if self.is_connected:
                logger.warning("File watcher is already connected")
                return True
            
            # Process existing files if configured
            if self.config.process_existing:
                self._scan_existing_files()
            
            # Set up file system watcher
            event_handler = FinancialStatementFileHandler(self)
            self.observer = Observer()
            self.observer.schedule(
                event_handler, 
                str(self.config.input_dir), 
                recursive=False
            )
            self.observer.start()
            
            self.is_connected = True
            logger.info(f"File watcher started for directory: {self.config.input_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start file watcher: {str(e)}")
            return False
    
    def _scan_existing_files(self):
        """Scan for existing JSON files in the input directory."""
        try:
            json_files = list(self.config.input_dir.glob("*.json"))
            
            # Sort by modification time (oldest first)
            json_files.sort(key=lambda x: x.stat().st_mtime)
            
            for file_path in json_files:
                if self._is_file_ready(file_path):
                    self.add_file_to_queue(file_path)
            
            if json_files:
                logger.info(f"Found {len(json_files)} existing JSON files to process")
                
        except Exception as e:
            logger.error(f"Error scanning existing files: {str(e)}")
    
    def _is_file_ready(self, file_path: Path) -> bool:
        """
        Check if file is ready for processing (not being written to).
        
        Args:
            file_path: Path to the file
            
        Returns:
            bool: True if file is ready
        """
        try:
            # Check if file size is stable (wait a bit and check again)
            initial_size = file_path.stat().st_size
            time.sleep(0.5)
            
            if not file_path.exists():  # File was moved/deleted
                return False
                
            final_size = file_path.stat().st_size
            
            # File is ready if size is stable and > 0
            return initial_size == final_size and final_size > 0
            
        except Exception as e:
            logger.error(f"Error checking file readiness {file_path}: {str(e)}")
            return False
    
    def add_file_to_queue(self, file_path: Path):
        """Add a file to the processing queue."""
        if file_path.exists() and file_path not in self.file_queue:
            self.file_queue.append(file_path)
            logger.debug(f"Added file to queue: {file_path}")
    
    def consume_messages(self, max_messages: int = 100) -> Generator[Dict, None, None]:
        """
        Consume messages from JSON files.
        
        Args:
            max_messages: Maximum number of messages to yield
            
        Yields:
            Dict: Financial statement message from JSON file
        """
        if not self.is_connected:
            if not self.connect():
                raise RuntimeError("Failed to establish file watcher connection")
        
        message_count = 0
        processed_files = []
        
        try:
            while message_count < max_messages and self.file_queue:
                file_path = self.file_queue.pop(0)
                
                try:
                    if not file_path.exists():
                        logger.warning(f"File no longer exists: {file_path}")
                        continue
                    
                    # Read and parse JSON file
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle both single objects and arrays
                    if isinstance(data, list):
                        for item in data:
                            if message_count >= max_messages:
                                break
                            yield item
                            message_count += 1
                    else:
                        yield data
                        message_count += 1
                    
                    processed_files.append(file_path)
                    logger.debug(f"Successfully processed file: {file_path}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in file {file_path}: {str(e)}")
                    self._move_file_to_error(file_path, f"JSON decode error: {str(e)}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")
                    self._move_file_to_error(file_path, f"Processing error: {str(e)}")
                    continue
            
            # Archive successfully processed files
            for file_path in processed_files:
                self._archive_file(file_path)
                
        except Exception as e:
            logger.error(f"Error during message consumption: {str(e)}")
            raise
    
    def consume_batch(self, batch_size: int = 10, timeout_ms: int = 5000) -> List[Dict]:
        """
        Consume a batch of messages with timeout.
        
        Args:
            batch_size: Number of messages to consume
            timeout_ms: Timeout in milliseconds
            
        Returns:
            List[Dict]: List of statement messages
        """
        messages = []
        start_time = time.time()
        timeout_seconds = timeout_ms / 1000.0
        
        try:
            # First, process any files already in queue
            for message in self.consume_messages(max_messages=batch_size):
                messages.append(message)
                if len(messages) >= batch_size:
                    break
            
            # If we need more messages and haven't timed out, wait for new files
            while len(messages) < batch_size and (time.time() - start_time) < timeout_seconds:
                if not self.file_queue:
                    time.sleep(self.config.polling_interval)
                    continue
                
                for message in self.consume_messages(max_messages=batch_size - len(messages)):
                    messages.append(message)
                    if len(messages) >= batch_size:
                        break
            
            logger.info(f"Consumed batch of {len(messages)} messages from files")
            return messages
            
        except Exception as e:
            logger.error(f"Error consuming batch: {str(e)}")
            raise
    
    def _archive_file(self, file_path: Path):
        """Move processed file to archive directory."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_name = f"{timestamp}_{file_path.name}"
            archive_path = self.config.archive_dir / archive_name
            
            shutil.move(str(file_path), str(archive_path))
            logger.debug(f"Archived file: {file_path} -> {archive_path}")
            
        except Exception as e:
            logger.error(f"Error archiving file {file_path}: {str(e)}")
            # If archiving fails, try to at least remove the original file
            try:
                file_path.unlink()
            except:
                pass
    
    def _move_file_to_error(self, file_path: Path, error_message: str):
        """Move problematic file to error directory."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            error_name = f"{timestamp}_{file_path.name}"
            error_path = self.config.error_dir / error_name
            
            shutil.move(str(file_path), str(error_path))
            
            # Create error report file
            error_report_path = error_path.with_suffix('.error.txt')
            with open(error_report_path, 'w') as f:
                f.write(f"Error processing file: {file_path.name}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Error: {error_message}\n")
            
            logger.error(f"Moved problematic file to error directory: {error_path}")
            
        except Exception as e:
            logger.error(f"Error moving file to error directory {file_path}: {str(e)}")
    
    def get_queue_size(self) -> int:
        """Get the current size of the file processing queue."""
        return len(self.file_queue)
    
    def get_statistics(self) -> Dict:
        """Get file watcher statistics."""
        try:
            input_files = len(list(self.config.input_dir.glob("*.json")))
            archived_files = len(list(self.config.archive_dir.glob("*.json")))
            error_files = len(list(self.config.error_dir.glob("*.json")))
            
            return {
                'is_connected': self.is_connected,
                'queue_size': self.get_queue_size(),
                'input_files_pending': input_files,
                'archived_files': archived_files,
                'error_files': error_files,
                'input_directory': str(self.config.input_dir),
                'archive_directory': str(self.config.archive_dir),
                'error_directory': str(self.config.error_dir)
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
            return {'error': str(e)}
    
    def close(self):
        """Stop the file watcher."""
        try:
            if self.observer and self.observer.is_alive():
                self.observer.stop()
                self.observer.join()
            
            self.is_connected = False
            logger.info("File watcher stopped")
            
        except Exception as e:
            logger.error(f"Error stopping file watcher: {str(e)}")


class StatementMessageProcessor:
    """
    Processor for handling individual financial statement messages from files.
    Maintains compatibility with Kafka message processor.
    """
    
    @staticmethod
    def extract_metadata(message: Dict) -> Dict:
        """
        Extract metadata from statement message.
        
        Args:
            message: Statement message from JSON file
            
        Returns:
            Dict: Extracted metadata
        """
        metadata = message.get('metadata', {})
        
        return {
            'statement_id': message.get('statement_id'),
            'customer_id': message.get('customer_id'),
            'statement_date': message.get('statement_date'),
            'statement_type': message.get('statement_type', 'monthly'),
            'template_name': metadata.get('template_name', 'monthly'),
            'template_version': metadata.get('template_version', '1.0'),
            'currency': metadata.get('currency', 'USD'),
            'processing_timestamp': metadata.get('processing_timestamp'),
            'source': 'file_watcher'  # Identify source
        }
    
    @staticmethod
    def extract_financial_data(message: Dict) -> Dict:
        """
        Extract financial data from statement message.
        
        Args:
            message: Statement message from JSON file
            
        Returns:
            Dict: Organized financial data
        """
        return {
            'customer_info': message.get('customer_info', {}),
            'account_summary': message.get('account_summary', {}),
            'transactions': message.get('transactions', []),
            'balances': message.get('balances', {}),
            'line_items': message.get('line_items', []),
            'totals': message.get('totals', {})
        }