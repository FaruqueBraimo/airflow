"""
Tests for data validation and transformation utilities.
"""
import pytest
from datetime import datetime
from utils.validation.statement_validator import (
    StatementValidator, 
    DataTransformer, 
    FinancialStatement,
    CustomerInfo,
    Transaction,
    Balance,
    StatementMetadata
)


class TestStatementValidator:
    """Test cases for StatementValidator."""
    
    def test_validate_valid_statement(self, statement_validator, sample_statement_data):
        """Test validation of valid statement data."""
        result = statement_validator.validate_statement_message(sample_statement_data)
        
        assert result is not None
        assert result['statement_id'] == sample_statement_data['statement_id']
        assert result['customer_id'] == sample_statement_data['customer_id']
        assert len(result['transactions']) == len(sample_statement_data['transactions'])
    
    def test_validate_invalid_statement(self, statement_validator, invalid_statement_data):
        """Test validation of invalid statement data."""
        result = statement_validator.validate_statement_message(invalid_statement_data)
        
        assert result is None
    
    def test_validate_empty_message(self, statement_validator):
        """Test validation of empty message."""
        result = statement_validator.validate_statement_message({})
        
        assert result is None
    
    def test_validate_missing_required_fields(self, statement_validator):
        """Test validation when required fields are missing."""
        incomplete_data = {
            "statement_id": "STMT-001",
            # Missing customer_id, customer_info, statement_date
        }
        
        result = statement_validator.validate_statement_message(incomplete_data)
        assert result is None
    
    def test_validate_customer_id_mismatch(self, statement_validator):
        """Test validation fails when customer IDs don't match."""
        data = {
            "statement_id": "STMT-001",
            "customer_id": "CUST-001",
            "statement_date": "2024-01-31T23:59:59Z",
            "customer_info": {
                "customer_id": "CUST-002",  # Different from statement customer_id
                "name": "Test Customer"
            },
            "transactions": [],
            "balances": {},
            "metadata": {}
        }
        
        result = statement_validator.validate_statement_message(data)
        assert result is None
    
    def test_validate_future_statement_date(self, statement_validator):
        """Test validation fails for future statement dates."""
        future_date = datetime.now().replace(year=datetime.now().year + 1)
        data = {
            "statement_id": "STMT-001", 
            "customer_id": "CUST-001",
            "statement_date": future_date.isoformat() + "Z",
            "customer_info": {
                "customer_id": "CUST-001",
                "name": "Test Customer"
            },
            "transactions": [],
            "balances": {},
            "metadata": {}
        }
        
        result = statement_validator.validate_statement_message(data)
        assert result is None
    
    def test_validate_transaction_amounts(self, statement_validator):
        """Test validation of transaction amounts."""
        data = {
            "statement_id": "STMT-001",
            "customer_id": "CUST-001", 
            "statement_date": "2024-01-31T23:59:59Z",
            "customer_info": {
                "customer_id": "CUST-001",
                "name": "Test Customer"
            },
            "transactions": [
                {
                    "transaction_id": "TXN-001",
                    "date": "2024-01-15T10:30:00Z",
                    "description": "Test Transaction",
                    "amount": "invalid-amount"  # Invalid amount
                }
            ],
            "balances": {},
            "metadata": {}
        }
        
        result = statement_validator.validate_statement_message(data)
        assert result is None


class TestDataTransformer:
    """Test cases for DataTransformer."""
    
    def test_normalize_amounts(self, data_transformer, sample_statement_data):
        """Test amount normalization."""
        # Add some amounts with extra precision
        test_data = sample_statement_data.copy()
        test_data['transactions'][0]['amount'] = 3500.123456789
        test_data['balances']['checking']['opening_balance'] = 1500.999
        
        result = data_transformer.normalize_amounts(test_data, precision=2)
        
        assert result['transactions'][0]['amount'] == 3500.12
        assert result['balances']['checking']['opening_balance'] == 1500.00
    
    def test_normalize_amounts_with_different_precision(self, data_transformer, sample_statement_data):
        """Test amount normalization with different precision."""
        test_data = sample_statement_data.copy()
        test_data['transactions'][0]['amount'] = 3500.123
        
        result = data_transformer.normalize_amounts(test_data, precision=1)
        
        assert result['transactions'][0]['amount'] == 3500.1
    
    def test_enrich_statement_data(self, data_transformer, sample_statement_data):
        """Test statement data enrichment."""
        result = data_transformer.enrich_statement_data(sample_statement_data)
        
        # Check that totals were calculated
        assert 'totals' in result
        assert 'total_credits' in result['totals']
        assert 'total_debits' in result['totals']
        assert 'net_change' in result['totals']
        assert 'transaction_count' in result['totals']
        
        # Check metadata enrichment
        assert 'processed_timestamp' in result['metadata']
        assert 'processor_version' in result['metadata']
        
        # Verify calculations
        assert result['totals']['total_credits'] == 3500.00
        assert result['totals']['total_debits'] == 214.75
        assert result['totals']['transaction_count'] == 3
    
    def test_enrich_empty_transactions(self, data_transformer):
        """Test enrichment with empty transactions list."""
        data = {
            "statement_id": "STMT-001",
            "transactions": [],
            "metadata": {}
        }
        
        result = data_transformer.enrich_statement_data(data)
        
        assert 'totals' in result
        assert result['totals']['total_credits'] == 0
        assert result['totals']['total_debits'] == 0
        assert result['totals']['transaction_count'] == 0
    
    def test_normalize_amounts_handles_missing_data(self, data_transformer):
        """Test normalization handles missing transaction/balance data."""
        data = {
            "statement_id": "STMT-001",
            # No transactions or balances
        }
        
        # Should not raise an exception
        result = data_transformer.normalize_amounts(data)
        assert result['statement_id'] == "STMT-001"


class TestPydanticModels:
    """Test cases for Pydantic data models."""
    
    def test_customer_info_model(self):
        """Test CustomerInfo model validation."""
        valid_data = {
            "customer_id": "CUST-001",
            "name": "John Doe",
            "email": "john@example.com"
        }
        
        customer = CustomerInfo(**valid_data)
        assert customer.customer_id == "CUST-001"
        assert customer.name == "John Doe"
        assert customer.email == "john@example.com"
    
    def test_customer_info_model_invalid(self):
        """Test CustomerInfo model with invalid data."""
        invalid_data = {
            "customer_id": "",  # Empty customer_id
            "name": "",  # Empty name
        }
        
        with pytest.raises(Exception):
            CustomerInfo(**invalid_data)
    
    def test_transaction_model(self):
        """Test Transaction model validation."""
        valid_data = {
            "transaction_id": "TXN-001",
            "date": "2024-01-15T10:30:00Z",
            "description": "Test Transaction",
            "amount": 100.50
        }
        
        transaction = Transaction(**valid_data)
        assert transaction.transaction_id == "TXN-001"
        assert transaction.amount == 100.50
    
    def test_transaction_model_invalid_date(self):
        """Test Transaction model with invalid date."""
        invalid_data = {
            "transaction_id": "TXN-001",
            "date": "invalid-date",
            "description": "Test Transaction", 
            "amount": 100.50
        }
        
        with pytest.raises(Exception):
            Transaction(**invalid_data)
    
    def test_balance_model(self):
        """Test Balance model validation."""
        valid_data = {
            "account_type": "checking",
            "opening_balance": 1000.00,
            "closing_balance": 1500.00
        }
        
        balance = Balance(**valid_data)
        assert balance.account_type == "checking"
        assert balance.opening_balance == 1000.00
        assert balance.closing_balance == 1500.00
        assert balance.currency == "USD"  # Default value
    
    def test_financial_statement_model(self, sample_statement_data):
        """Test FinancialStatement model validation."""
        statement = FinancialStatement(**sample_statement_data)
        
        assert statement.statement_id == sample_statement_data['statement_id']
        assert statement.customer_id == sample_statement_data['customer_id']
        assert len(statement.transactions) == 3
        assert len(statement.balances) == 2
    
    def test_financial_statement_model_defaults(self):
        """Test FinancialStatement model with minimal required data."""
        minimal_data = {
            "statement_id": "STMT-001",
            "customer_id": "CUST-001",
            "statement_date": "2024-01-31T23:59:59Z",
            "customer_info": {
                "customer_id": "CUST-001",
                "name": "Test Customer"
            }
        }
        
        statement = FinancialStatement(**minimal_data)
        
        assert statement.statement_type == "monthly"  # Default value
        assert statement.transactions == []  # Default empty list
        assert statement.balances == {}  # Default empty dict
        assert statement.metadata.template_name == "monthly"  # Default value