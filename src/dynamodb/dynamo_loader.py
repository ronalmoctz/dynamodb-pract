import pandas as pd
from decimal import Decimal
from typing import List, Dict, Any
from .dynamo_client import get_dynamodb_resource

def convert_float_to_decimal(item: Dict[str, Any]) -> Dict[str, Any]:
    """DynamoDB does not support float, so we convert to Decimal."""
    new_item = {}
    for k, v in item.items():
        if isinstance(v, float):
            new_item[k] = Decimal(str(v))
        else:
            new_item[k] = v
    return new_item

def prepare_dataframe_for_dynamo(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Prepares DataFrame for DynamoDB:
    1. Ensure column names match schema (InvoiceNo, InvoiceDate, Country, CustomerID, TotalAmount)
    2. Convert types (dates to string, floats to Decimal)
    3. Handle missing values
    """
    df_clean = df.copy()
    
    # Ensure required columns exist (case insensitive mapping if needed, but assuming data_cleaning did its job)
    # Mapping based on typical dataset:
    # invoice_no -> InvoiceNo
    # invoice_date -> InvoiceDate
    # country -> Country
    # customer_id -> CustomerID
    # total_amount -> TotalAmount
    
    column_map = {
        'invoiceno': 'InvoiceNo',
        'stockcode': 'StockCode',
        'invoicedate': 'InvoiceDate',
        'country': 'Country',
        'customerid': 'CustomerID',
        'total_amount': 'TotalAmount',
        'quantity': 'Quantity',
        'unitprice': 'UnitPrice'
    }
    
    # Rename columns if they exist in lowercase
    df_clean.rename(columns=column_map, inplace=True)
    
    # Ensure InvoiceDate is string ISO format
    if 'InvoiceDate' in df_clean.columns:
        df_clean['InvoiceDate'] = pd.to_datetime(df_clean['InvoiceDate']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Fill NaN for CustomerID if missing
    if 'CustomerID' in df_clean.columns:
         df_clean['CustomerID'] = df_clean['CustomerID'].fillna('Guest').astype(str)
    else:
        df_clean['CustomerID'] = 'Guest'

    # Drop rows where PK is missing
    df_clean.dropna(subset=['InvoiceNo'], inplace=True)
    df_clean['InvoiceNo'] = df_clean['InvoiceNo'].astype(str)
    
    # AGGREGATE DUPLICATES to avoid DynamoDB Composite Key collisions
    # (InvoiceNo + StockCode) must be unique
    print("   Grouping duplicates by (InvoiceNo, StockCode)...")
    df_clean = df_clean.groupby(['InvoiceNo', 'StockCode'], as_index=False).agg({
        'Quantity': 'sum',
        'TotalAmount': 'sum',
        'InvoiceDate': 'first',
        'Country': 'first',
        'CustomerID': 'first',
        'UnitPrice': 'first'  # Assuming price is constant for same item in same invoice
    })

    # Convert to list of dicts
    records = df_clean.to_dict('records')
    
    # Convert floats to decimals
    return [convert_float_to_decimal(record) for record in records]

def load_orders_to_dynamodb(df: pd.DataFrame, table_name: str = "Ecommerce_eu"):
    """
    Loads DataFrame into DynamoDB using batch writer.
    """
    print(f"\n🚀 Starting DynamoDB Load to table: {table_name}")
    
    # We assume the table is already created via bash script
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)
    
    items = prepare_dataframe_for_dynamo(df)
    total_items = len(items)
    
    print(f"📦 Prepared {total_items} items for loading...")
    
    with table.batch_writer() as batch:
        for i, item in enumerate(items, 1):
            batch.put_item(Item=item)
            if i % 1000 == 0:
                print(f"   Processed {i}/{total_items} items...")
                
    print(f"✅ Successfully loaded {total_items} items to DynamoDB.")