import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import seaborn as sns
from typing import List, Dict, Any
import os

# Ensure plots directory exists
PLOTS_DIR = "plots"
os.makedirs(PLOTS_DIR, exist_ok=True)

# Set global aesthetic
sns.set_theme(style="whitegrid", context="talk")

def plot_sales_trend(data: List[Dict[str, Any]], title: str, filename: str):
    """
    Plots a highly aesthetic line chart showing sales trend over time.
    """
    if not data:
        print(f"⚠️ No data provided for plot: {title}")
        return

    df = pd.DataFrame(data)
    
    # Ensure types
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['TotalAmount'] = df['TotalAmount'].astype(float)
    
    # Group by Date
    daily_sales = df.groupby(df['InvoiceDate'].dt.date)['TotalAmount'].sum().reset_index()
    daily_sales.columns = ['Date', 'Total Sales']
    daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])
    
    plt.figure(figsize=(12, 6))
    
    # Plot line with gradient-like fill or just clean line
    ax = sns.lineplot(
        data=daily_sales, 
        x='Date', 
        y='Total Sales', 
        marker='o', 
        linewidth=2.5,
        color='#4c72b0'
    )
    
    # 1. Format X-Axis (Dates: 01 Dec)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2)) # Adjust interval as needed
    plt.xticks(rotation=45)
    
    # 2. Format Y-Axis (Thousands: 10k)
    def thousands_formatter(x, pos):
        return f'{x/1000:.1f}k'
    
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(thousands_formatter))
    
    # Aesthetics
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Sales (£)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    sns.despine(left=True, bottom=True)
    
    plt.tight_layout()
    
    output_path = os.path.join(PLOTS_DIR, filename)
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"📉 Aesthetic Chart saved: {output_path}")

def plot_order_distribution(data: List[Dict[str, Any]], title: str, filename: str):
    """
    Plots a histogram showing the distribution of order amounts.
    """
    if not data:
        print(f"⚠️ No data provided for plot: {title}")
        return

    df = pd.DataFrame(data)
    df['TotalAmount'] = df['TotalAmount'].astype(float)
    
    plt.figure(figsize=(10, 6))
    
    # Histogram with KDE
    sns.histplot(
        data=df, 
        x='TotalAmount', 
        kde=True, 
        bins=30, 
        color='#55a868',
        edgecolor='white'
    )
    
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel("Order Amount (£)", fontsize=12)
    plt.ylabel("Frequency", fontsize=12)
    sns.despine()
    plt.tight_layout()
    
    output_path = os.path.join(PLOTS_DIR, filename)
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"📊 Aesthetic Chart saved: {output_path}")
