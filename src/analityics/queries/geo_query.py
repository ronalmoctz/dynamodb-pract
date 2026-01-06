"""
Geographic visualization query for bubble map.
"""
import logging
from typing import List, Dict, Any, Optional
from boto3.dynamodb.conditions import Attr

from ..base.query_base import paginated_scan

logger = logging.getLogger(__name__)
DEFAULT_TABLE = "Ecommerce_eu"


def get_sales_for_geo_visualization(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    table_name: str = DEFAULT_TABLE
) -> List[Dict[str, Any]]:
    """
    Retrieves sales data for geographic bubble map visualization.
    
    Returns data with: Country, InvoiceDate, TotalAmount, InvoiceNo
    
    Args:
        start_date: Optional start date filter.
        end_date: Optional end date filter.
        table_name: DynamoDB table name.
        
    Returns:
        List of order items for geo visualization.
    """
    filter_expr = None
    
    if start_date and end_date:
        filter_expr = Attr('InvoiceDate').between(start_date, end_date)
    elif start_date:
        filter_expr = Attr('InvoiceDate').gte(start_date)
    elif end_date:
        filter_expr = Attr('InvoiceDate').lte(end_date)
    
    return paginated_scan(
        table_name=table_name,
        projection='Country, InvoiceDate, TotalAmount, InvoiceNo',
        filter_expression=filter_expr
    )
