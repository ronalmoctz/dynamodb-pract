import logging
from typing import List, Dict, Any, Optional
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import sys
import os

# Add src to path if needed for imports relative to root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.dynamodb.dynamo_client import get_table

logger = logging.getLogger(__name__)

def get_orders_by_client(customer_id: str, table_name: str = "Ecommerce_eu") -> List[Dict[str, Any]]:
    """
    Retrieves all orders for a specific client using the CustomerIndex.
    
    Args:
        customer_id: The ID of the customer.
        table_name: DynamoDB table name.
        
    Returns:
        List of order items.
    """
    table = get_table(table_name)
    try:
        response = table.query(
            IndexName='CustomerIndex',
            KeyConditionExpression=Key('CustomerID').eq(str(customer_id))
        )
        return response.get('Items', [])
    except Exception as e:
        logger.error(f"Error querying orders by client {customer_id}: {e}")
        return []

def get_sales_by_country(
    country: str, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None, 
    table_name: str = "Ecommerce_eu"
) -> List[Dict[str, Any]]:
    """
    Retrieves sales for a specific country using the CountryIndex.
    Optionally filters by a date range using the sort key (InvoiceDate).
    
    Args:
        country: Country name.
        start_date: Start date (ISO string).
        end_date: End date (ISO string).
        table_name: DynamoDB table name.
        
    Returns:
        List of sales items.
    """
    table = get_table(table_name)
    try:
        key_condition = Key('Country').eq(country)
        
        if start_date and end_date:
            key_condition = key_condition & Key('InvoiceDate').between(start_date, end_date)
            
        response = table.query(
            IndexName='CountryIndex',
            KeyConditionExpression=key_condition
        )
        return response.get('Items', [])
    except Exception as e:
        logger.error(f"Error querying sales by country {country}: {e}")
        return []

def get_orders_by_date_range(
    start_date: str, 
    end_date: str, 
    table_name: str = "Ecommerce_eu"
) -> List[Dict[str, Any]]:
    """
    Retrieves orders across ALL countries/clients within a date range.
    Uses a SCAN operation with FilterExpression (less efficient than Query).
    
    Args:
        start_date: Start date (ISO string).
        end_date: End date (ISO string).
        table_name: DynamoDB table name.
        
    Returns:
        List of order items.
    """
    table = get_table(table_name)
    try:
        scan_kwargs = {
            'FilterExpression': Attr('InvoiceDate').between(start_date, end_date)
        }
        
        items = []
        done = False
        start_key = None
        
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
                
            response = table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
            
        return items
    except Exception as e:
        logger.error(f"Error scanning orders by date range: {e}")
        return []

def calculate_revenue_by_date(target_date: str, table_name: str = "Ecommerce_eu") -> float:
    """
    Calculates total revenue for a specific date (YYYY-MM-DD).
    
    Args:
        target_date: Date string (YYYY-MM-DD).
        table_name: DynamoDB table name.
        
    Returns:
        Total revenue as float.
    """
    # Define start and end of day in ISO format
    start_ts = f"{target_date}T00:00:00"
    end_ts = f"{target_date}T23:59:59"
    
    orders = get_orders_by_date_range(start_ts, end_ts, table_name)
    
    total_revenue = sum(float(item.get('TotalAmount', 0)) for item in orders)
    return total_revenue
