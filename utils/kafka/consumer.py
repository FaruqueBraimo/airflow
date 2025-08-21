"""
Kafka consumer module for financial statement data processing.
"""
import json
import logging
from typing import Dict, List, Optional, Generator
from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError
from config.airflow_config import KAFKA_CONFIG
from utils.validation.statement_validator import StatementValidator

logger = logging.getLogger(__name__)


class FinancialStatementConsumer:
    """
    Kafka consumer for processing financial statement messages.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize the Kafka consumer with configuration."""
        self.config = config or KAFKA_CONFIG
        self.consumer = None
        self.validator = StatementValidator()
        
    def connect(self) -> bool:
        """
        Establish connection to Kafka cluster.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.consumer = KafkaConsumer(
                self.config['topic'],
                bootstrap_servers=self.config['bootstrap_servers'],
                group_id=self.config['group_id'],
                auto_offset_reset=self.config['auto_offset_reset'],
                enable_auto_commit=self.config['enable_auto_commit'],
                consumer_timeout_ms=self.config['consumer_timeout_ms'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            logger.info(f"Connected to Kafka cluster at {self.config['bootstrap_servers']}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            return False
    
    def consume_messages(self, max_messages: int = 100) -> Generator[Dict, None, None]:
        """
        Consume messages from Kafka topic.
        
        Args:
            max_messages: Maximum number of messages to consume in one batch
            
        Yields:
            Dict: Validated financial statement message
        """
        if not self.consumer:
            if not self.connect():
                raise RuntimeError("Failed to establish Kafka connection")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                if message_count >= max_messages:
                    break
                    
                try:
                    # Validate message structure
                    validated_data = self.validator.validate_statement_message(
                        message.value
                    )
                    
                    if validated_data:
                        logger.debug(f"Processing message from partition {message.partition}, "
                                   f"offset {message.offset}")
                        yield validated_data
                        message_count += 1
                    else:
                        logger.warning(f"Invalid message format in partition {message.partition}, "
                                     f"offset {message.offset}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {str(e)}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
                    
        except KafkaTimeoutError:
            logger.info("Consumer timeout reached, no new messages available")
        except KafkaError as e:
            logger.error(f"Kafka error during consumption: {str(e)}")
            raise
            
    def consume_batch(self, batch_size: int = 10, timeout_ms: int = 5000) -> List[Dict]:
        """
        Consume a batch of messages with timeout.
        
        Args:
            batch_size: Number of messages to consume
            timeout_ms: Timeout in milliseconds
            
        Returns:
            List[Dict]: List of validated statement messages
        """
        messages = []
        
        try:
            for message in self.consume_messages(max_messages=batch_size):
                messages.append(message)
                
            logger.info(f"Successfully consumed {len(messages)} messages")
            return messages
            
        except Exception as e:
            logger.error(f"Error consuming batch: {str(e)}")
            raise
    
    def close(self):
        """Close the Kafka consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer connection closed")


class StatementMessageProcessor:
    """
    Processor for handling individual financial statement messages.
    """
    
    @staticmethod
    def extract_metadata(message: Dict) -> Dict:
        """
        Extract metadata from statement message.
        
        Args:
            message: Raw statement message
            
        Returns:
            Dict: Extracted metadata including template info
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
            'processing_timestamp': metadata.get('processing_timestamp')
        }
    
    @staticmethod
    def extract_financial_data(message: Dict) -> Dict:
        """
        Extract financial data from statement message.
        
        Args:
            message: Raw statement message
            
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