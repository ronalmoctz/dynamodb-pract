"""
Base plot utilities providing DRY patterns for visualizations.

This module centralizes:
- Output directory management
- Color palettes and themes
- Country ISO code mappings
- Figure saving utilities (HTML + PNG)
"""
import os
from typing import Dict, Optional

import seaborn as sns

# ============================================================
# DIRECTORY CONFIGURATION
# ============================================================
PLOTS_DIR = "plots"
os.makedirs(PLOTS_DIR, exist_ok=True)

# ============================================================
# THEME & COLOR PALETTES
# ============================================================
sns.set_theme(style="whitegrid", context="talk")

# Premium color palette for charts
COLOR_PALETTE = [
    '#3498db',  # Blue
    '#e74c3c',  # Red  
    '#2ecc71',  # Green
    '#9b59b6',  # Purple
    '#f39c12',  # Orange
    '#1abc9c',  # Teal
    '#e91e63',  # Pink
    '#00bcd4',  # Cyan
]

THEME_COLORS = {
    'primary': '#4c72b0',
    'success': '#55a868',
    'background': '#ffffff',
    'text': '#2c3e50',
    'grid': '#ecf0f1',
    'border': '#bdc3c7',
}

# ============================================================
# COUNTRY ISO MAPPINGS (for geographic plots)
# ============================================================
COUNTRY_ISO_MAP: Dict[str, Optional[str]] = {
    'United Kingdom': 'GBR',
    'France': 'FRA',
    'Germany': 'DEU',
    'Spain': 'ESP',
    'Portugal': 'PRT',
    'Italy': 'ITA',
    'Netherlands': 'NLD',
    'Belgium': 'BEL',
    'Switzerland': 'CHE',
    'Austria': 'AUT',
    'Norway': 'NOR',
    'Sweden': 'SWE',
    'Denmark': 'DNK',
    'Finland': 'FIN',
    'Ireland': 'IRL',
    'Poland': 'POL',
    'Greece': 'GRC',
    'Australia': 'AUS',
    'Japan': 'JPN',
    'USA': 'USA',
    'Canada': 'CAN',
    'Brazil': 'BRA',
    'Singapore': 'SGP',
    'Hong Kong': 'HKG',
    'United Arab Emirates': 'ARE',
    'Israel': 'ISR',
    'Cyprus': 'CYP',
    'Malta': 'MLT',
    'Iceland': 'ISL',
    'Czech Republic': 'CZE',
    'Lithuania': 'LTU',
    'Channel Islands': 'GBR',
    'EIRE': 'IRL',
    'RSA': 'ZAF',
    'Saudi Arabia': 'SAU',
    'Bahrain': 'BHR',
    'Lebanon': 'LBN',
    'European Community': None,
    'Unspecified': None,
}


# ============================================================
# SAVE UTILITIES
# ============================================================
def save_plotly_figure(fig, filename: str) -> Dict[str, str]:
    """
    Saves a Plotly figure to both HTML (interactive) and PNG (static) formats.
    
    Args:
        fig: Plotly figure object.
        filename: Base filename (extension will be replaced).
        
    Returns:
        Dict with paths: {'html': path, 'png': path or None}
    """
    base_name = filename.rsplit('.', 1)[0]
    
    html_path = os.path.join(PLOTS_DIR, f"{base_name}.html")
    png_path = os.path.join(PLOTS_DIR, f"{base_name}.png")
    
    result = {'html': html_path, 'png': None}
    
    # Save interactive HTML (always works)
    fig.write_html(html_path, include_plotlyjs='cdn', full_html=True)
    print(f"🌐 Interactive saved: {html_path}")
    
    # Try to save static PNG (requires Chrome/Kaleido)
    try:
        fig.write_image(png_path, width=1400, height=800, scale=2)
        result['png'] = png_path
        print(f"🖼️  Static saved: {png_path}")
    except Exception as e:
        print(f"⚠️  PNG export skipped (run 'plotly_get_chrome' to enable): {type(e).__name__}")
    
    return result


def get_output_path(filename: str) -> str:
    """Returns full path for a plot filename."""
    return os.path.join(PLOTS_DIR, filename)
