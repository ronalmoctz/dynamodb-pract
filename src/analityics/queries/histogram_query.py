"""
Histogram analysis query for order distribution.
"""
import logging
from typing import List, Dict, Any, Optional
from decimal import Decimal
from boto3.dynamodb.conditions import Attr

from ..base.query_base import paginated_scan

logger = logging.getLogger(__name__)
DEFAULT_TABLE = "Ecommerce_eu"


def get_sales_for_histogram_analysis(
    countries: Optional[List[str]] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    table_name: str = DEFAULT_TABLE
) -> List[Dict[str, Any]]:
    """
    Retrieves sales data for histogram analysis by country.
    
    Returns data with: Country, TotalAmount, CustomerID
    
    Args:
        countries: Optional list of countries to include.
        min_amount: Optional minimum order amount.
        max_amount: Optional maximum order amount.
        table_name: DynamoDB table name.
        
    Returns:
        List of order items for histogram analysis.
    """
    conditions = []
    
    if min_amount is not None:
        conditions.append(Attr('TotalAmount').gte(Decimal(str(min_amount))))
    
    if max_amount is not None:
        conditions.append(Attr('TotalAmount').lte(Decimal(str(max_amount))))
    
    if countries:
        country_conds = [Attr('Country').eq(c) for c in countries]
        combined_country = country_conds[0]
        for cond in country_conds[1:]:
            combined_country = combined_country | cond
        conditions.append(combined_country)
    
    # Combine all conditions with AND
    filter_expr = None
    if conditions:
        filter_expr = conditions[0]
        for cond in conditions[1:]:
            filter_expr = filter_expr & cond
    
    return paginated_scan(
        table_name=table_name,
        projection='Country, TotalAmount, CustomerID',
        filter_expression=filter_expr
    )
