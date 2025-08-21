"""
Data validation utilities for financial statement processing.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pydantic import BaseModel, Field, validator
from jsonschema import validate, ValidationError
import json

logger = logging.getLogger(__name__)


class CustomerInfo(BaseModel):
    """Customer information model."""
    customer_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    address: Optional[Dict[str, str]] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class Transaction(BaseModel):
    """Individual transaction model."""
    transaction_id: str = Field(..., min_length=1)
    date: str = Field(...)
    description: str = Field(..., min_length=1)
    amount: float = Field(...)
    category: Optional[str] = None
    reference: Optional[str] = None
    
    @validator('date')
    def validate_date(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('Invalid date format. Use ISO 8601 format.')


class Balance(BaseModel):
    """Account balance model."""
    account_type: str = Field(..., min_length=1)
    opening_balance: float = Field(...)
    closing_balance: float = Field(...)
    currency: str = Field(default='USD', min_length=3, max_length=3)


class StatementMetadata(BaseModel):
    """Statement metadata model."""
    template_name: str = Field(default='monthly')
    template_version: str = Field(default='1.0')
    currency: str = Field(default='USD', min_length=3, max_length=3)
    processing_timestamp: Optional[str] = None


class FinancialStatement(BaseModel):
    """Complete financial statement model."""
    statement_id: str = Field(..., min_length=1)
    customer_id: str = Field(..., min_length=1)
    statement_date: str = Field(...)
    statement_type: str = Field(default='monthly')
    customer_info: CustomerInfo
    transactions: List[Transaction] = Field(default_factory=list)
    balances: Dict[str, Balance] = Field(default_factory=dict)
    totals: Dict[str, float] = Field(default_factory=dict)
    metadata: StatementMetadata = Field(default_factory=StatementMetadata)
    
    @validator('statement_date')
    def validate_statement_date(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('Invalid statement_date format. Use ISO 8601 format.')


class StatementValidator:
    """
    Comprehensive validator for financial statement data.
    """
    
    def __init__(self):
        """Initialize the validator with schema definitions."""
        self.schema = self._load_json_schema()
        
    def _load_json_schema(self) -> Dict:
        """Load JSON schema for statement validation."""
        return {
            "type": "object",
            "required": ["statement_id", "customer_id", "statement_date", "customer_info"],
            "properties": {
                "statement_id": {"type": "string", "minLength": 1},
                "customer_id": {"type": "string", "minLength": 1},
                "statement_date": {"type": "string", "format": "date-time"},
                "statement_type": {"type": "string", "enum": ["monthly", "quarterly", "annual"]},
                "customer_info": {
                    "type": "object",
                    "required": ["customer_id", "name"],
                    "properties": {
                        "customer_id": {"type": "string", "minLength": 1},
                        "name": {"type": "string", "minLength": 1},
                        "address": {"type": "object"},
                        "email": {"type": "string", "format": "email"},
                        "phone": {"type": "string"}
                    }
                },
                "transactions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["transaction_id", "date", "description", "amount"],
                        "properties": {
                            "transaction_id": {"type": "string", "minLength": 1},
                            "date": {"type": "string", "format": "date-time"},
                            "description": {"type": "string", "minLength": 1},
                            "amount": {"type": "number"},
                            "category": {"type": "string"},
                            "reference": {"type": "string"}
                        }
                    }
                },
                "balances": {"type": "object"},
                "totals": {"type": "object"},
                "metadata": {
                    "type": "object",
                    "properties": {
                        "template_name": {"type": "string"},
                        "template_version": {"type": "string"},
                        "currency": {"type": "string", "minLength": 3, "maxLength": 3},
                        "processing_timestamp": {"type": "string", "format": "date-time"}
                    }
                }
            }
        }
    
    def validate_statement_message(self, message: Dict) -> Optional[Dict]:
        """
        Validate a complete statement message.
        
        Args:
            message: Raw statement message from Kafka
            
        Returns:
            Optional[Dict]: Validated message or None if invalid
        """
        try:
            # First, validate against JSON schema
            validate(instance=message, schema=self.schema)
            
            # Then validate using Pydantic model for detailed validation
            statement = FinancialStatement(**message)
            
            # Additional custom validations
            if not self._validate_amounts(statement):
                logger.error("Amount validation failed")
                return None
                
            if not self._validate_business_rules(statement):
                logger.error("Business rule validation failed")
                return None
            
            logger.debug(f"Successfully validated statement {statement.statement_id}")
            return statement.dict()
            
        except ValidationError as e:
            logger.error(f"JSON schema validation error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Statement validation error: {str(e)}")
            return None
    
    def _validate_amounts(self, statement: FinancialStatement) -> bool:
        """
        Validate monetary amounts are properly formatted.
        
        Args:
            statement: Validated statement object
            
        Returns:
            bool: True if amounts are valid
        """
        try:
            # Validate transaction amounts
            for transaction in statement.transactions:
                if not isinstance(transaction.amount, (int, float)):
                    return False
                    
                # Check for reasonable amount ranges (optional business rule)
                if abs(transaction.amount) > 1_000_000:  # $1M limit
                    logger.warning(f"Large transaction amount: {transaction.amount}")
            
            # Validate balance amounts
            for balance_data in statement.balances.values():
                if isinstance(balance_data, dict):
                    for key in ['opening_balance', 'closing_balance']:
                        if key in balance_data:
                            if not isinstance(balance_data[key], (int, float)):
                                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Amount validation error: {str(e)}")
            return False
    
    def _validate_business_rules(self, statement: FinancialStatement) -> bool:
        """
        Apply business-specific validation rules.
        
        Args:
            statement: Validated statement object
            
        Returns:
            bool: True if business rules are satisfied
        """
        try:
            # Rule 1: Statement date should not be in the future
            statement_date = datetime.fromisoformat(
                statement.statement_date.replace('Z', '+00:00')
            )
            if statement_date > datetime.now(statement_date.tzinfo):
                logger.error("Statement date cannot be in the future")
                return False
            
            # Rule 2: Customer ID should match between statement and customer_info
            if statement.customer_id != statement.customer_info.customer_id:
                logger.error("Customer ID mismatch between statement and customer_info")
                return False
            
            # Rule 3: Template name should be supported
            supported_templates = ['monthly', 'quarterly', 'annual']
            if statement.metadata.template_name not in supported_templates:
                logger.warning(f"Unsupported template: {statement.metadata.template_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Business rule validation error: {str(e)}")
            return False


class DataTransformer:
    """
    Transform and normalize financial statement data.
    """
    
    @staticmethod
    def normalize_amounts(data: Dict, precision: int = 2) -> Dict:
        """
        Normalize monetary amounts to specified precision.
        
        Args:
            data: Statement data
            precision: Decimal places for amounts
            
        Returns:
            Dict: Data with normalized amounts
        """
        try:
            # Normalize transaction amounts
            if 'transactions' in data:
                for transaction in data['transactions']:
                    if 'amount' in transaction:
                        transaction['amount'] = round(float(transaction['amount']), precision)
            
            # Normalize balances
            if 'balances' in data:
                for balance_key, balance_data in data['balances'].items():
                    if isinstance(balance_data, dict):
                        for amount_key in ['opening_balance', 'closing_balance']:
                            if amount_key in balance_data:
                                balance_data[amount_key] = round(
                                    float(balance_data[amount_key]), precision
                                )
            
            # Normalize totals
            if 'totals' in data:
                for total_key, total_value in data['totals'].items():
                    if isinstance(total_value, (int, float)):
                        data['totals'][total_key] = round(float(total_value), precision)
            
            return data
            
        except Exception as e:
            logger.error(f"Amount normalization error: {str(e)}")
            raise
    
    @staticmethod
    def enrich_statement_data(data: Dict) -> Dict:
        """
        Enrich statement data with calculated fields.
        
        Args:
            data: Statement data
            
        Returns:
            Dict: Enriched statement data
        """
        try:
            # Calculate transaction summary
            if 'transactions' in data and data['transactions']:
                total_credits = sum(
                    t['amount'] for t in data['transactions'] 
                    if t['amount'] > 0
                )
                total_debits = sum(
                    abs(t['amount']) for t in data['transactions'] 
                    if t['amount'] < 0
                )
                transaction_count = len(data['transactions'])
                
                # Add summary to totals
                if 'totals' not in data:
                    data['totals'] = {}
                    
                data['totals'].update({
                    'total_credits': total_credits,
                    'total_debits': total_debits,
                    'transaction_count': transaction_count,
                    'net_change': total_credits - total_debits
                })
            
            # Add processing metadata
            if 'metadata' not in data:
                data['metadata'] = {}
                
            data['metadata']['processed_timestamp'] = datetime.utcnow().isoformat()
            data['metadata']['processor_version'] = '1.0.0'
            
            return data
            
        except Exception as e:
            logger.error(f"Data enrichment error: {str(e)}")
            raise