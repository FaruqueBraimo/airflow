"""
Pytest configuration and shared fixtures for the financial statement pipeline tests.
"""
import pytest
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, MagicMock
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.validation.statement_validator import StatementValidator, DataTransformer
from utils.templates.template_manager import TemplateManager
from utils.pdf.pdf_generator import PDFGenerator
from utils.kafka.consumer import FinancialStatementConsumer
from utils.monitoring.pipeline_monitor import PipelineMonitor


@pytest.fixture
def sample_statement_data():
    """Sample valid financial statement data for testing."""
    return {
        "statement_id": "STMT-2024-001",
        "customer_id": "CUST-12345",
        "statement_date": "2024-01-31T23:59:59Z",
        "statement_type": "monthly",
        "customer_info": {
            "customer_id": "CUST-12345",
            "name": "John Doe",
            "address": {
                "street": "123 Main St",
                "city": "Anytown", 
                "state": "CA",
                "zip": "12345"
            },
            "email": "john.doe@email.com",
            "phone": "+1-555-123-4567"
        },
        "transactions": [
            {
                "transaction_id": "TXN-001",
                "date": "2024-01-15T10:30:00Z",
                "description": "Direct Deposit - Salary",
                "amount": 3500.00,
                "category": "Income",
                "reference": "PAY-001"
            },
            {
                "transaction_id": "TXN-002", 
                "date": "2024-01-20T14:15:00Z",
                "description": "Grocery Store Purchase",
                "amount": -125.50,
                "category": "Food",
                "reference": "POS-002"
            },
            {
                "transaction_id": "TXN-003",
                "date": "2024-01-25T09:00:00Z",
                "description": "Utility Bill Payment",
                "amount": -89.25,
                "category": "Utilities",
                "reference": "BILL-003"
            }
        ],
        "balances": {
            "checking": {
                "opening_balance": 1500.00,
                "closing_balance": 4785.25,
                "currency": "USD"
            },
            "savings": {
                "opening_balance": 5000.00,
                "closing_balance": 5000.00,
                "currency": "USD"
            }
        },
        "totals": {
            "total_credits": 3500.00,
            "total_debits": 214.75,
            "net_change": 3285.25,
            "transaction_count": 3
        },
        "metadata": {
            "template_name": "monthly",
            "template_version": "1.0",
            "currency": "USD",
            "processing_timestamp": "2024-01-31T23:59:59Z"
        }
    }


@pytest.fixture
def invalid_statement_data():
    """Sample invalid financial statement data for testing."""
    return {
        "statement_id": "",  # Invalid: empty string
        "customer_id": "CUST-12345",
        "statement_date": "invalid-date",  # Invalid: bad date format
        "customer_info": {
            "customer_id": "DIFFERENT-ID",  # Invalid: doesn't match statement customer_id
            "name": ""  # Invalid: empty name
        },
        "transactions": [
            {
                "transaction_id": "TXN-001",
                "date": "2024-01-15T10:30:00Z",
                "description": "",  # Invalid: empty description
                "amount": "not-a-number"  # Invalid: not a number
            }
        ]
    }


@pytest.fixture
def sample_kafka_messages():
    """Sample Kafka messages for testing."""
    return [
        {
            "statement_id": "STMT-001",
            "customer_id": "CUST-001",
            "statement_date": "2024-01-31T23:59:59Z",
            "statement_type": "monthly",
            "customer_info": {
                "customer_id": "CUST-001", 
                "name": "Alice Johnson",
                "email": "alice@example.com"
            },
            "transactions": [],
            "balances": {},
            "metadata": {"template_name": "monthly"}
        },
        {
            "statement_id": "STMT-002",
            "customer_id": "CUST-002",
            "statement_date": "2024-01-31T23:59:59Z", 
            "statement_type": "quarterly",
            "customer_info": {
                "customer_id": "CUST-002",
                "name": "Bob Smith"
            },
            "transactions": [],
            "balances": {},
            "metadata": {"template_name": "quarterly"}
        }
    ]


@pytest.fixture
def temp_output_dir():
    """Temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_templates_dir():
    """Temporary directory for test templates."""
    with tempfile.TemporaryDirectory() as temp_dir:
        templates_dir = Path(temp_dir)
        
        # Create sample template structure
        monthly_dir = templates_dir / "monthly"
        monthly_dir.mkdir(parents=True)
        
        # Create simple test template
        template_content = """
        <!DOCTYPE html>
        <html>
        <head><title>{{ statement_type | title }} Statement</title></head>
        <body>
            <h1>{{ customer_info.name }}</h1>
            <p>Statement ID: {{ statement_id }}</p>
            <p>Date: {{ statement_date }}</p>
        </body>
        </html>
        """
        
        (monthly_dir / "template.html").write_text(template_content)
        
        yield templates_dir


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer for testing."""
    mock_consumer = Mock(spec=FinancialStatementConsumer)
    mock_consumer.connect.return_value = True
    mock_consumer.consume_batch.return_value = []
    mock_consumer.close.return_value = None
    return mock_consumer


@pytest.fixture
def statement_validator():
    """StatementValidator instance for testing."""
    return StatementValidator()


@pytest.fixture
def data_transformer():
    """DataTransformer instance for testing.""" 
    return DataTransformer()


@pytest.fixture 
def template_manager(temp_templates_dir):
    """TemplateManager instance with temporary templates."""
    return TemplateManager(templates_dir=temp_templates_dir)


@pytest.fixture
def pdf_generator(temp_output_dir):
    """PDFGenerator instance with temporary output directory."""
    return PDFGenerator(output_dir=temp_output_dir)


@pytest.fixture
def pipeline_monitor():
    """PipelineMonitor instance for testing."""
    return PipelineMonitor()


@pytest.fixture
def airflow_context():
    """Mock Airflow context for testing DAG tasks."""
    context = {
        'execution_date': datetime(2024, 1, 31),
        'ds': '2024-01-31',
        'ds_nodash': '20240131',
        'task_instance': Mock()
    }
    
    # Configure task instance XCom methods
    context['task_instance'].xcom_push = Mock()
    context['task_instance'].xcom_pull = Mock()
    
    return context


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Automatically set up test environment for all tests."""
    # Ensure test directories exist
    test_data_dir = Path(__file__).parent / "data"
    test_data_dir.mkdir(exist_ok=True)
    
    # Configure logging to reduce noise during tests
    import logging
    logging.getLogger('kafka').setLevel(logging.CRITICAL)
    logging.getLogger('reportlab').setLevel(logging.CRITICAL)
    logging.getLogger('weasyprint').setLevel(logging.CRITICAL)
    
    yield
    
    # Cleanup after tests if needed


@pytest.fixture
def sample_pdf_content():
    """Sample PDF content for testing."""
    return b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"


# Helper functions for tests
def create_test_kafka_message(**overrides):
    """Create a test Kafka message with optional overrides."""
    base_message = {
        "statement_id": "TEST-001",
        "customer_id": "CUST-TEST",
        "statement_date": "2024-01-31T23:59:59Z",
        "statement_type": "monthly",
        "customer_info": {
            "customer_id": "CUST-TEST",
            "name": "Test Customer"
        },
        "transactions": [],
        "balances": {},
        "metadata": {"template_name": "monthly"}
    }
    
    base_message.update(overrides)
    return base_message


def create_test_pdf_file(file_path: Path, content: bytes = None):
    """Create a test PDF file."""
    if content is None:
        content = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
    
    file_path.write_bytes(content)
    return file_path