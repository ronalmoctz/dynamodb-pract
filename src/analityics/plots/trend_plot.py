"""
Sales trend line chart visualization.
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import seaborn as sns
from typing import List, Dict, Any

from ..base.plot_base import PLOTS_DIR, THEME_COLORS, get_output_path


def plot_sales_trend(data: List[Dict[str, Any]], title: str, filename: str):
    """
    Plots a line chart showing sales trend over time.
    
    Args:
        data: List of order records with InvoiceDate and TotalAmount.
        title: Chart title.
        filename: Output filename.
    """
    if not data:
        print(f"⚠️ No data for plot: {title}")
        return

    df = pd.DataFrame(data)
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['TotalAmount'] = df['TotalAmount'].astype(float)
    
    # Aggregate by date
    daily = df.groupby(df['InvoiceDate'].dt.date)['TotalAmount'].sum().reset_index()
    daily.columns = ['Date', 'Total Sales']
    daily['Date'] = pd.to_datetime(daily['Date'])
    
    # Create plot
    plt.figure(figsize=(12, 6))
    ax = sns.lineplot(
        data=daily,
        x='Date',
        y='Total Sales',
        marker='o',
        linewidth=2.5,
        color=THEME_COLORS['primary']
    )
    
    # Format axes
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    plt.xticks(rotation=45)
    
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{x/1000:.1f}k'))
    
    # Style
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Sales (£)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    sns.despine(left=True, bottom=True)
    plt.tight_layout()
    
    # Save
    output_path = get_output_path(filename)
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"📉 Chart saved: {output_path}")
