"""
Environment setup utilities for the financial statement pipeline.
"""
import os
import sys
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def setup_environment():
    """
    Set up the environment for the financial statement processing pipeline.
    """
    # Set Python path
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # Set environment variables
    os.environ.setdefault('PYTHONPATH', str(project_root))
    
    # Configure logging
    setup_logging()
    
    logger.info("Environment setup completed")


def setup_logging(log_level: str = "INFO"):
    """
    Configure logging for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Create logs directory
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logs_dir / 'pipeline.log'),
            logging.StreamHandler()
        ]
    )
    
    # Set specific loggers
    logging.getLogger('kafka').setLevel(logging.WARNING)
    logging.getLogger('reportlab').setLevel(logging.WARNING)
    logging.getLogger('weasyprint').setLevel(logging.WARNING)


def validate_dependencies():
    """
    Validate that all required dependencies are available.
    
    Returns:
        bool: True if all dependencies are available
    """
    required_packages = [
        'kafka',
        'reportlab', 
        'weasyprint',
        'pydantic',
        'jinja2',
        'jsonschema'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"Missing required packages: {', '.join(missing_packages)}")
        logger.error("Please install missing packages using: pip install -r requirements.txt")
        return False
    
    logger.info("All required dependencies are available")
    return True


def create_required_directories():
    """
    Create all required directories for the pipeline.
    """
    project_root = Path(__file__).parent.parent
    
    required_dirs = [
        'logs',
        'output',
        'templates/monthly',
        'templates/quarterly', 
        'templates/annual',
        'tests/data',
        'config'
    ]
    
    for dir_path in required_dirs:
        full_path = project_root / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {full_path}")
    
    logger.info("All required directories created")


def get_environment_info() -> Dict[str, str]:
    """
    Get information about the current environment.
    
    Returns:
        Dict with environment information
    """
    project_root = Path(__file__).parent.parent
    
    return {
        'python_version': sys.version,
        'project_root': str(project_root),
        'working_directory': os.getcwd(),
        'environment': os.environ.get('ENVIRONMENT', 'development'),
        'kafka_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'log_level': os.environ.get('LOG_LEVEL', 'INFO')
    }


if __name__ == '__main__':
    setup_environment()
    create_required_directories()
    
    if validate_dependencies():
        logger.info("Environment setup completed successfully")
        env_info = get_environment_info()
        for key, value in env_info.items():
            logger.info(f"{key}: {value}")
    else:
        logger.error("Environment setup failed")
        sys.exit(1)