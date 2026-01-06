"""
Order distribution histogram visualization.
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Any

from ..base.plot_base import THEME_COLORS, get_output_path


def plot_order_distribution(data: List[Dict[str, Any]], title: str, filename: str):
    """
    Plots a histogram showing order amount distribution.
    
    Args:
        data: List of order records with TotalAmount.
        title: Chart title.
        filename: Output filename.
    """
    if not data:
        print(f"⚠️ No data for plot: {title}")
        return

    df = pd.DataFrame(data)
    df['TotalAmount'] = df['TotalAmount'].astype(float)
    
    # Create plot
    plt.figure(figsize=(10, 6))
    sns.histplot(
        data=df,
        x='TotalAmount',
        kde=True,
        bins=30,
        color=THEME_COLORS['success'],
        edgecolor='white'
    )
    
    # Style
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel("Order Amount (£)", fontsize=12)
    plt.ylabel("Frequency", fontsize=12)
    sns.despine()
    plt.tight_layout()
    
    # Save
    output_path = get_output_path(filename)
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"📊 Chart saved: {output_path}")
