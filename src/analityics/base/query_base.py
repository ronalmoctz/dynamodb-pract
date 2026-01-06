"""
Base query utilities providing DRY patterns for DynamoDB operations.

This module reduces code duplication by extracting common patterns:
- Paginated scans and queries
- Table reference caching
- Standard error handling
"""
import logging
from typing import List, Dict, Any, Optional, Callable
from boto3.dynamodb.conditions import ConditionBase

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.dynamodb.dynamo_client import get_table

logger = logging.getLogger(__name__)


def paginated_scan(
    table_name: str,
    projection: Optional[str] = None,
    filter_expression: Optional[ConditionBase] = None,
) -> List[Dict[str, Any]]:
    """
    Executes a paginated DynamoDB scan with automatic handling of LastEvaluatedKey.
    
    Args:
        table_name: DynamoDB table name.
        projection: Comma-separated list of attributes to retrieve.
        filter_expression: Optional boto3 FilterExpression.
        
    Returns:
        List of all matching items.
    """
    table = get_table(table_name)
    
    scan_kwargs = {}
    if projection:
        scan_kwargs['ProjectionExpression'] = projection
    if filter_expression:
        scan_kwargs['FilterExpression'] = filter_expression
    
    items = []
    start_key = None
    
    try:
        while True:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
                
            response = table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            
            start_key = response.get('LastEvaluatedKey')
            if not start_key:
                break
                
        logger.info(f"Scan completed: {len(items)} items retrieved")
        return items
        
    except Exception as e:
        logger.error(f"Paginated scan error: {e}")
        return []


def paginated_query(
    table_name: str,
    index_name: str,
    key_condition: ConditionBase,
    projection: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Executes a paginated DynamoDB query on an index.
    
    Args:
        table_name: DynamoDB table name.
        index_name: GSI name to query.
        key_condition: boto3 KeyConditionExpression.
        projection: Optional comma-separated attributes.
        
    Returns:
        List of all matching items.
    """
    table = get_table(table_name)
    
    query_kwargs = {
        'IndexName': index_name,
        'KeyConditionExpression': key_condition
    }
    if projection:
        query_kwargs['ProjectionExpression'] = projection
    
    items = []
    start_key = None
    
    try:
        while True:
            if start_key:
                query_kwargs['ExclusiveStartKey'] = start_key
                
            response = table.query(**query_kwargs)
            items.extend(response.get('Items', []))
            
            start_key = response.get('LastEvaluatedKey')
            if not start_key:
                break
                
        logger.info(f"Query completed: {len(items)} items retrieved")
        return items
        
    except Exception as e:
        logger.error(f"Paginated query error: {e}")
        return []
