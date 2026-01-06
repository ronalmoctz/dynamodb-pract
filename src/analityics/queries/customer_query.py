"""
Customer-related DynamoDB queries.
"""
import logging
from typing import List, Dict, Any
from boto3.dynamodb.conditions import Key

from ..base.query_base import paginated_query

logger = logging.getLogger(__name__)
DEFAULT_TABLE = "Ecommerce_eu"


def get_orders_by_client(
    customer_id: str,
    table_name: str = DEFAULT_TABLE
) -> List[Dict[str, Any]]:
    """
    Retrieves all orders for a specific customer using CustomerIndex.
    
    Args:
        customer_id: The customer ID.
        table_name: DynamoDB table name.
        
    Returns:
        List of order items.
    """
    key_condition = Key('CustomerID').eq(str(customer_id))
    
    return paginated_query(
        table_name=table_name,
        index_name='CustomerIndex',
        key_condition=key_condition
    )
