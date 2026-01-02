"""Utilidades para el proyecto dynamodb-pract"""

from .tablate import (
    print_dataframe_head,
    print_sales_summary,
    print_country_distribution,
    print_full_report
)

__all__ = [
    'print_dataframe_head',
    'print_sales_summary',
    'print_country_distribution',
    'print_full_report'
]
