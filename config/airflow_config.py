"""
Airflow configuration settings for financial statement processing pipeline.
"""
import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
TEMPLATES_DIR = PROJECT_ROOT / "templates"
LOGS_DIR = PROJECT_ROOT / "logs"
OUTPUT_DIR = PROJECT_ROOT / "output"

# Data source configuration
DATA_SOURCE_TYPE = os.getenv('DATA_SOURCE', 'file_watcher')  # 'kafka' or 'file_watcher'

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'topic': os.getenv('KAFKA_TOPIC', 'financial-statements'),
    'group_id': os.getenv('KAFKA_GROUP_ID', 'airflow-statement-processor'),
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'consumer_timeout_ms': 10000,
}

# File watcher configuration
FILE_WATCHER_CONFIG = {
    'input_dir': os.getenv('INPUT_DIR', PROJECT_ROOT / 'input'),
    'archive_dir': os.getenv('ARCHIVE_DIR', PROJECT_ROOT / 'archive'),
    'error_dir': os.getenv('ERROR_DIR', PROJECT_ROOT / 'error'),
    'file_pattern': os.getenv('FILE_PATTERN', '*.json'),
    'process_existing': os.getenv('PROCESS_EXISTING', 'true').lower() == 'true',
    'batch_size': int(os.getenv('FILE_BATCH_SIZE', '50')),
    'polling_interval': int(os.getenv('FILE_POLLING_INTERVAL', '5')),
}

# PDF generation settings
PDF_CONFIG = {
    'page_size': 'letter',
    'margins': {
        'top': 72,
        'bottom': 72, 
        'left': 72,
        'right': 72
    },
    'font_size': {
        'title': 16,
        'heading': 12,
        'body': 10
    }
}

# Template configuration
TEMPLATE_CONFIG = {
    'default_template': 'monthly',
    'default_version': '1.0',
    'supported_formats': ['monthly', 'quarterly', 'annual'],
    'template_cache_ttl': 3600  # 1 hour
}

# Airflow DAG default arguments
DEFAULT_DAG_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay_minutes': 5,
}

# Monitoring and alerting
MONITORING_CONFIG = {
    'enable_metrics': True,
    'metrics_port': 8080,
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'alert_channels': ['email', 'slack']  # Configure as needed
}