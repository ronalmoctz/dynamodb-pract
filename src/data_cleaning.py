import pandas as pd
from typing import List


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizes column names to lower case, stripped, and snake_case.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with normalized column names.
    """
    df = df.copy()
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )
    return df


def clean_dates(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Converts a column to datetime and drops rows with invalid dates.

    Args:
        df: Input DataFrame.
        column: Name of the column containing dates.

    Returns:
        DataFrame with the column converted to datetime and NaT rows dropped.
    """
    df = df.copy()
    if column not in df.columns:
        print(f"⚠️ Warning: Column '{column}' not found. Available columns: {df.columns.tolist()}")
        return df
    
    df[column] = pd.to_datetime(df[column], errors="coerce")
    df = df.dropna(subset=[column])
    return df


def clean_numeric(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Converts specified columns to numeric and drops rows with NaN values.

    Args:
        df: Input DataFrame.
        columns: List of column names to convert.

    Returns:
        DataFrame with numeric columns and without NaN values in those columns.
    """
    df = df.copy()
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=columns)
    return df


def remove_invalid_sales(df: pd.DataFrame, quantity_col: str = 'quantity', price_col: str = 'unitprice') -> pd.DataFrame:
    """Removes rows with non-positive sales (quantity or price <= 0).

    Args:
        df: Input DataFrame.
        quantity_col: Name of the quantity column.
        price_col: Name of the unit price column.

    Returns:
        Filtered DataFrame.
    """
    df = df.copy()
    # Ensure columns exist before filtering
    if quantity_col in df.columns and price_col in df.columns:
        df = df[(df[quantity_col] > 0) & (df[price_col] > 0)]
    return df


def add_total_amount(df: pd.DataFrame, quantity_col: str = 'quantity', price_col: str = 'unitprice') -> pd.DataFrame:
    """Adds a 'total_amount' column (quantity * unitprice).

    Args:
        df: Input DataFrame.
        quantity_col: Name of the quantity column.
        price_col: Name of the unit price column.

    Returns:
        DataFrame with the new 'total_amount' column.
    """
    df = df.copy()
    if quantity_col in df.columns and price_col in df.columns:
        df['total_amount'] = df[quantity_col] * df[price_col]
    return df