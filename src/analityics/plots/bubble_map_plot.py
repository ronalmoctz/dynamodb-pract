"""
Animated bubble map for geographic sales visualization.
"""
import pandas as pd
import plotly.express as px
from typing import List, Dict, Any

from ..base.plot_base import COUNTRY_ISO_MAP, THEME_COLORS, save_plotly_figure


def plot_sales_bubble_map(data: List[Dict[str, Any]], title: str, filename: str):
    """
    Creates an animated bubble map showing sales by country over time.
    
    Features:
    - Bubble size = total revenue
    - Bubble color = order count
    - Animation by month
    
    Args:
        data: List of order records with Country, InvoiceDate, TotalAmount, InvoiceNo.
        title: Chart title.
        filename: Base filename (saves .html and .png).
    """
    if not data:
        print(f"⚠️ No data for plot: {title}")
        return

    df = pd.DataFrame(data)
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['TotalAmount'] = df['TotalAmount'].astype(float)
    df['YearMonth'] = df['InvoiceDate'].dt.to_period('M').astype(str)
    
    # Map countries to ISO codes
    df['iso_alpha'] = df['Country'].map(COUNTRY_ISO_MAP)
    df = df.dropna(subset=['iso_alpha'])
    
    if df.empty:
        print(f"⚠️ No valid country mappings for: {title}")
        return
    
    # Aggregate by country and month
    geo = df.groupby(['Country', 'iso_alpha', 'YearMonth']).agg({
        'TotalAmount': 'sum',
        'InvoiceNo': 'nunique'
    }).reset_index()
    geo.columns = ['Country', 'iso_alpha', 'YearMonth', 'TotalSales', 'OrderCount']
    geo = geo.sort_values('YearMonth')
    
    # Create figure
    fig = px.scatter_geo(
        geo,
        locations='iso_alpha',
        size='TotalSales',
        color='OrderCount',
        hover_name='Country',
        hover_data={'TotalSales': ':,.2f', 'OrderCount': ':,', 'iso_alpha': False},
        animation_frame='YearMonth',
        projection='natural earth',
        title=title,
        color_continuous_scale='Viridis',
        size_max=60,
        labels={'TotalSales': 'Sales (£)', 'OrderCount': 'Orders', 'YearMonth': 'Period'}
    )
    
    # Style
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'font': {'size': 20, 'color': THEME_COLORS['text']}},
        geo=dict(
            showframe=False,
            showcoastlines=True,
            coastlinecolor=THEME_COLORS['border'],
            showland=True,
            landcolor=THEME_COLORS['grid'],
            showocean=True,
            oceancolor='#d4e6f1',
            showcountries=True,
            countrycolor='#95a5a6'
        ),
        paper_bgcolor=THEME_COLORS['background'],
        margin={'l': 20, 'r': 20, 't': 80, 'b': 20}
    )
    
    fig.update_traces(marker=dict(line=dict(width=1, color=THEME_COLORS['text']), opacity=0.8))
    
    # Save
    save_plotly_figure(fig, filename)
