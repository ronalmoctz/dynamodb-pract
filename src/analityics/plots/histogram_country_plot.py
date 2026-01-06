"""
Multi-country histogram for order amount distribution comparison.
"""
import pandas as pd
import plotly.express as px
from typing import List, Dict, Any

from ..base.plot_base import COLOR_PALETTE, THEME_COLORS, save_plotly_figure


def plot_amount_histogram_by_country(
    data: List[Dict[str, Any]],
    title: str,
    filename: str,
    top_n_countries: int = 5
):
    """
    Creates overlapping histograms comparing order amounts by country.
    
    Features:
    - Layered histograms with transparency
    - Box plots for distribution summary
    - Statistics annotation
    
    Args:
        data: List of order records with Country and TotalAmount.
        title: Chart title.
        filename: Base filename (saves .html and .png).
        top_n_countries: Number of top countries to display.
    """
    if not data:
        print(f"⚠️ No data for plot: {title}")
        return

    df = pd.DataFrame(data)
    df['TotalAmount'] = df['TotalAmount'].astype(float)
    df = df[df['TotalAmount'] > 0]  # Exclude refunds
    
    if df.empty:
        print(f"⚠️ No positive transactions for: {title}")
        return
    
    # Get top countries
    top_countries = df['Country'].value_counts().head(top_n_countries).index.tolist()
    df_filtered = df[df['Country'].isin(top_countries)]
    
    # Create figure
    fig = px.histogram(
        df_filtered,
        x='TotalAmount',
        color='Country',
        nbins=50,
        opacity=0.7,
        barmode='overlay',
        title=title,
        color_discrete_sequence=COLOR_PALETTE,
        labels={'TotalAmount': 'Order Amount (£)', 'count': 'Orders'},
        marginal='box'
    )
    
    # Style
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'font': {'size': 20, 'color': THEME_COLORS['text']}},
        xaxis_title={'text': 'Order Amount (£)', 'font': {'size': 14}},
        yaxis_title={'text': 'Number of Orders', 'font': {'size': 14}},
        paper_bgcolor=THEME_COLORS['background'],
        plot_bgcolor='#fafafa',
        legend={
            'title': {'text': 'Country'},
            'orientation': 'h',
            'y': -0.2,
            'x': 0.5,
            'xanchor': 'center'
        },
        margin={'l': 60, 'r': 40, 't': 100, 'b': 100}
    )
    
    fig.update_xaxes(showgrid=True, gridcolor=THEME_COLORS['grid'])
    fig.update_yaxes(showgrid=True, gridcolor=THEME_COLORS['grid'])
    
    # Add stats annotation
    stats = df_filtered.groupby('Country')['TotalAmount'].agg(['mean', 'count'])
    stats_text = "<b>Mean Order Value:</b><br>"
    for country in top_countries[:3]:
        if country in stats.index:
            stats_text += f"{country}: £{stats.loc[country, 'mean']:.2f}<br>"
    
    fig.add_annotation(
        text=stats_text,
        xref="paper", yref="paper",
        x=0.98, y=0.98,
        showarrow=False,
        font={'size': 10, 'color': '#7f8c8d'},
        align='right',
        bgcolor='rgba(255,255,255,0.9)',
        borderpad=8
    )
    
    # Save
    save_plotly_figure(fig, filename)
