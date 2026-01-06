"""
Sales-related DynamoDB queries.

Provides queries for:
- Sales by country (using CountryIndex)
- Orders by date range
- Revenue calculations
"""
import logging
from typing import List, Dict, Any, Optional
from boto3.dynamodb.conditions import Key, Attr

from ..base.query_base import paginated_scan, paginated_query

logger = logging.getLogger(__name__)
DEFAULT_TABLE = "Ecommerce_eu"


def get_sales_by_country(
    country: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    table_name: str = DEFAULT_TABLE
) -> List[Dict[str, Any]]:
    """
    Retrieves sales for a specific country using CountryIndex.
    
    Args:
        country: Country name.
        start_date: Optional start date (ISO format).
        end_date: Optional end date (ISO format).
        table_name: DynamoDB table name.
        
    Returns:
        List of sales items.
    """
    key_condition = Key('Country').eq(country)
    
    if start_date and end_date:
        key_condition = key_condition & Key('InvoiceDate').between(start_date, end_date)
    
    return paginated_query(
        table_name=table_name,
        index_name='CountryIndex',
        key_condition=key_condition
    )


def get_orders_by_date_range(
    start_date: str,
    end_date: str,
    table_name: str = DEFAULT_TABLE
) -> List[Dict[str, Any]]:
    """
    Retrieves orders within a date range using table scan.
    
    Args:
        start_date: Start date (ISO format).
        end_date: End date (ISO format).
        table_name: DynamoDB table name.
        
    Returns:
        List of order items.
    """
    filter_expr = Attr('InvoiceDate').between(start_date, end_date)
    
    return paginated_scan(
        table_name=table_name,
        filter_expression=filter_expr
    )


def calculate_revenue_by_date(
    target_date: str,
    table_name: str = DEFAULT_TABLE
) -> float:
    """
    Calculates total revenue for a specific date.
    
    Args:
        target_date: Date string (YYYY-MM-DD).
        table_name: DynamoDB table name.
        
    Returns:
        Total revenue as float.
    """
    start_ts = f"{target_date}T00:00:00"
    end_ts = f"{target_date}T23:59:59"
    
    orders = get_orders_by_date_range(start_ts, end_ts, table_name)
    
    return sum(float(item.get('TotalAmount', 0)) for item in orders)
