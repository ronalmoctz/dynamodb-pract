"""
Analytics module for e-commerce data visualization and analysis.

This module provides:
- DynamoDB queries for sales, customer, and geo data
- Visualizations including trend charts, histograms, and bubble maps

Example:
    from analityics import (
        get_sales_by_country,
        get_sales_for_geo_visualization,
        plot_sales_trend,
        plot_sales_bubble_map
    )
    
    # Get data and visualize
    uk_sales = get_sales_by_country('United Kingdom')
    plot_sales_trend(uk_sales, 'UK Sales Trend', 'uk_trend.png')
"""

# Queries
from .queries import (
    get_sales_by_country,
    get_orders_by_date_range,
    calculate_revenue_by_date,
    get_orders_by_client,
    get_sales_for_geo_visualization,
    get_sales_for_histogram_analysis,
)

# Plots
from .plots import (
    plot_sales_trend,
    plot_order_distribution,
    plot_sales_bubble_map,
    plot_amount_histogram_by_country,
)

__all__ = [
    # Queries
    'get_sales_by_country',
    'get_orders_by_date_range',
    'calculate_revenue_by_date',
    'get_orders_by_client',
    'get_sales_for_geo_visualization',
    'get_sales_for_histogram_analysis',
    # Plots
    'plot_sales_trend',
    'plot_order_distribution',
    'plot_sales_bubble_map',
    'plot_amount_histogram_by_country',
]
