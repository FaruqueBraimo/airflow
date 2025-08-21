"""
Data source factory for switching between Kafka and file watcher data sources.
"""
import logging
from typing import Dict, List, Optional, Union, Protocol
from enum import Enum
from abc import ABC, abstractmethod

from utils.kafka.consumer import FinancialStatementConsumer as KafkaConsumer
from utils.file_watcher.file_watcher import FinancialStatementFileWatcher, FileWatcherConfig
from config.airflow_config import KAFKA_CONFIG

logger = logging.getLogger(__name__)


class DataSourceType(Enum):
    """Enumeration of supported data source types."""
    KAFKA = "kafka"
    FILE_WATCHER = "file_watcher"


class DataSourceInterface(Protocol):
    """Protocol defining the interface for data sources."""
    
    def connect(self) -> bool:
        """Establish connection to the data source."""
        ...
    
    def consume_batch(self, batch_size: int = 10, timeout_ms: int = 5000) -> List[Dict]:
        """Consume a batch of messages."""
        ...
    
    def close(self):
        """Close the data source connection."""
        ...


class BaseDataSource(ABC):
    """Base class for data sources with common functionality."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.is_connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    def consume_batch(self, batch_size: int = 10, timeout_ms: int = 5000) -> List[Dict]:
        """Consume a batch of messages."""
        pass
    
    @abstractmethod
    def close(self):
        """Close the data source connection.""" 
        pass
    
    def get_status(self) -> Dict:
        """Get data source status."""
        return {
            'is_connected': self.is_connected,
            'type': self.__class__.__name__
        }


class KafkaDataSource(BaseDataSource):
    """Kafka data source wrapper."""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.consumer = KafkaConsumer(config)
        logger.info("Initialized Kafka data source")
    
    def connect(self) -> bool:
        """Connect to Kafka cluster."""
        try:
            result = self.consumer.connect()
            self.is_connected = result
            return result
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            return False
    
    def consume_batch(self, batch_size: int = 10, timeout_ms: int = 5000) -> List[Dict]:
        """Consume batch from Kafka."""
        try:
            return self.consumer.consume_batch(batch_size, timeout_ms)
        except Exception as e:
            logger.error(f"Error consuming from Kafka: {str(e)}")
            raise
    
    def close(self):
        """Close Kafka connection."""
        try:
            self.consumer.close()
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error closing Kafka connection: {str(e)}")
    
    def get_status(self) -> Dict:
        """Get Kafka-specific status."""
        base_status = super().get_status()
        base_status.update({
            'type': 'kafka',
            'bootstrap_servers': self.config.get('bootstrap_servers', []),
            'topic': self.config.get('topic', ''),
            'group_id': self.config.get('group_id', '')
        })
        return base_status


class FileWatcherDataSource(BaseDataSource):
    """File watcher data source wrapper."""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        
        # Create FileWatcherConfig from dict
        file_config = FileWatcherConfig(
            input_dir=config.get('input_dir', 'input'),
            archive_dir=config.get('archive_dir', 'archive'),
            error_dir=config.get('error_dir', 'error'),
            file_pattern=config.get('file_pattern', '*.json'),
            process_existing=config.get('process_existing', True),
            batch_size=config.get('batch_size', 50),
            polling_interval=config.get('polling_interval', 5)
        )
        
        self.watcher = FinancialStatementFileWatcher(file_config)
        logger.info(f"Initialized file watcher data source for directory: {config.get('input_dir', 'input')}")
    
    def connect(self) -> bool:
        """Start file watcher."""
        try:
            result = self.watcher.connect()
            self.is_connected = result
            return result
        except Exception as e:
            logger.error(f"Failed to start file watcher: {str(e)}")
            return False
    
    def consume_batch(self, batch_size: int = 10, timeout_ms: int = 5000) -> List[Dict]:
        """Consume batch from files."""
        try:
            return self.watcher.consume_batch(batch_size, timeout_ms)
        except Exception as e:
            logger.error(f"Error consuming from file watcher: {str(e)}")
            raise
    
    def close(self):
        """Stop file watcher."""
        try:
            self.watcher.close()
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error closing file watcher: {str(e)}")
    
    def get_status(self) -> Dict:
        """Get file watcher specific status."""
        base_status = super().get_status()
        base_status.update({
            'type': 'file_watcher',
            **self.watcher.get_statistics()
        })
        return base_status
    
    def get_queue_size(self) -> int:
        """Get current queue size."""
        return self.watcher.get_queue_size()


class DataSourceFactory:
    """
    Factory for creating and managing data sources.
    """
    
    def __init__(self):
        self._registered_sources = {
            DataSourceType.KAFKA: KafkaDataSource,
            DataSourceType.FILE_WATCHER: FileWatcherDataSource
        }
        logger.info("Data source factory initialized")
    
    def create_data_source(
        self, 
        source_type: Union[DataSourceType, str], 
        config: Optional[Dict] = None
    ) -> BaseDataSource:
        """
        Create a data source of the specified type.
        
        Args:
            source_type: Type of data source to create
            config: Configuration for the data source
            
        Returns:
            BaseDataSource: Configured data source instance
        """
        try:
            # Convert string to enum if needed
            if isinstance(source_type, str):
                source_type = DataSourceType(source_type.lower())
            
            if source_type not in self._registered_sources:
                raise ValueError(f"Unsupported data source type: {source_type}")
            
            # Get default config based on source type
            if config is None:
                config = self._get_default_config(source_type)
            
            # Create and return data source
            source_class = self._registered_sources[source_type]
            data_source = source_class(config)
            
            logger.info(f"Created {source_type.value} data source")
            return data_source
            
        except Exception as e:
            logger.error(f"Error creating data source {source_type}: {str(e)}")
            raise
    
    def _get_default_config(self, source_type: DataSourceType) -> Dict:
        """Get default configuration for a data source type."""
        if source_type == DataSourceType.KAFKA:
            return KAFKA_CONFIG.copy()
        elif source_type == DataSourceType.FILE_WATCHER:
            return {
                'input_dir': 'input',
                'archive_dir': 'archive', 
                'error_dir': 'error',
                'file_pattern': '*.json',
                'process_existing': True,
                'batch_size': 50,
                'polling_interval': 5
            }
        else:
            return {}
    
    def create_from_environment(self, data_source_env_var: str = "DATA_SOURCE") -> BaseDataSource:
        """
        Create data source based on environment variable.
        
        Args:
            data_source_env_var: Environment variable name containing source type
            
        Returns:
            BaseDataSource: Configured data source
        """
        import os
        
        source_type_str = os.environ.get(data_source_env_var, "file_watcher")
        
        logger.info(f"Creating data source from environment: {source_type_str}")
        
        try:
            source_type = DataSourceType(source_type_str.lower())
        except ValueError:
            logger.warning(f"Invalid data source type '{source_type_str}', defaulting to file_watcher")
            source_type = DataSourceType.FILE_WATCHER
        
        return self.create_data_source(source_type)
    
    def get_available_sources(self) -> List[str]:
        """Get list of available data source types."""
        return [source_type.value for source_type in self._registered_sources.keys()]
    
    def register_source(self, source_type: DataSourceType, source_class: type):
        """
        Register a new data source type.
        
        Args:
            source_type: Type enum for the data source
            source_class: Class implementing BaseDataSource
        """
        if not issubclass(source_class, BaseDataSource):
            raise ValueError("Source class must inherit from BaseDataSource")
        
        self._registered_sources[source_type] = source_class
        logger.info(f"Registered new data source type: {source_type.value}")


class DataSourceManager:
    """
    Manager for handling data source lifecycle and switching.
    """
    
    def __init__(self):
        self.factory = DataSourceFactory()
        self.current_source: Optional[BaseDataSource] = None
        self.current_type: Optional[DataSourceType] = None
    
    def initialize_source(
        self, 
        source_type: Union[DataSourceType, str], 
        config: Optional[Dict] = None,
        auto_connect: bool = True
    ) -> bool:
        """
        Initialize a data source.
        
        Args:
            source_type: Type of data source
            config: Optional configuration
            auto_connect: Whether to automatically connect
            
        Returns:
            bool: True if initialization successful
        """
        try:
            # Close existing source if any
            if self.current_source:
                self.current_source.close()
            
            # Create new source
            self.current_source = self.factory.create_data_source(source_type, config)
            
            if isinstance(source_type, str):
                source_type = DataSourceType(source_type.lower())
            self.current_type = source_type
            
            # Auto-connect if requested
            if auto_connect:
                success = self.current_source.connect()
                if not success:
                    logger.error(f"Failed to connect to {source_type.value} data source")
                    return False
            
            logger.info(f"Successfully initialized {source_type.value} data source")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing data source: {str(e)}")
            return False
    
    def switch_source(
        self, 
        new_source_type: Union[DataSourceType, str], 
        config: Optional[Dict] = None
    ) -> bool:
        """
        Switch to a different data source type.
        
        Args:
            new_source_type: New data source type
            config: Optional configuration
            
        Returns:
            bool: True if switch successful
        """
        logger.info(f"Switching data source from {self.current_type} to {new_source_type}")
        return self.initialize_source(new_source_type, config, auto_connect=True)
    
    def get_current_source(self) -> Optional[BaseDataSource]:
        """Get the current active data source."""
        return self.current_source
    
    def get_status(self) -> Dict:
        """Get status of current data source."""
        if not self.current_source:
            return {'status': 'no_source_initialized'}
        
        return {
            'current_type': self.current_type.value if self.current_type else None,
            'source_status': self.current_source.get_status(),
            'available_sources': self.factory.get_available_sources()
        }
    
    def consume_batch(self, batch_size: int = 10, timeout_ms: int = 5000) -> List[Dict]:
        """Consume batch from current data source."""
        if not self.current_source:
            raise RuntimeError("No data source initialized")
        
        return self.current_source.consume_batch(batch_size, timeout_ms)
    
    def close(self):
        """Close current data source."""
        if self.current_source:
            self.current_source.close()
            self.current_source = None
            self.current_type = None


# Global data source manager instance
data_source_manager = DataSourceManager()