# src/data_save.py

import io
import boto3
import pandas as pd
from datetime import datetime
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION


def get_s3_client():
    """Creates a configured S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


def save_dataframe_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    file_format: str = "csv"
) -> dict:
    """Saves a DataFrame to S3 in CSV or Parquet format.

    Args:
        df: DataFrame to save.
        bucket: S3 bucket name.
        key: S3 file path (e.g., 'cleaned/data.csv').
        file_format: 'csv' or 'parquet'.

    Returns:
        dict: Information about the saved file.
    """
    s3 = get_s3_client()
    
    try:
        print(f"\n💾 Saving data to S3...")
        print(f"   Bucket: {bucket}")
        print(f"   Key: {key}")
        print(f"   Format: {file_format}")
        
        # Convert DataFrame based on format
        if file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            content = buffer.getvalue()
            content_type = "text/csv"
            
        elif file_format == "parquet":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            content = buffer.getvalue()
            content_type = "application/octet-stream"
            
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        # Upload to S3
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
            Metadata={
                'rows': str(len(df)),
                'columns': str(len(df.columns)),
                'uploaded_at': datetime.now().isoformat()
            }
        )
        
        # Calculate size
        size_mb = len(content) / (1024 * 1024)
        
        result = {
            "success": True,
            "s3_uri": f"s3://{bucket}/{key}",
            "size_mb": round(size_mb, 2),
            "rows": len(df),
            "columns": len(df.columns)
        }
        
        print(f"✅ File saved successfully")
        print(f"   URI: s3://{bucket}/{key}")
        print(f"   Size: {size_mb:.2f} MB")
        print(f"   Rows: {len(df):,}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error saving to S3: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def save_with_timestamp(
    df: pd.DataFrame,
    bucket: str,
    prefix: str = "cleaned",
    base_name: str = "ecommerce",
    file_format: str = "csv"
) -> dict:
    """Saves file with timestamp in the name. Useful for historical versions.

    Args:
        df: DataFrame to save.
        bucket: S3 bucket name.
        prefix: S3 folder prefix (e.g., 'cleaned').
        base_name: Base filename.
        file_format: 'csv' or 'parquet'.

    Returns:
        dict: Information about the saved file.

    Example:
        cleaned/ecommerce_2025-01-03_143022.csv
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{base_name}_{timestamp}.{file_format}"
    key = f"{prefix}/{filename}"
    
    return save_dataframe_to_s3(df, bucket, key, file_format)


def save_latest(
    df: pd.DataFrame,
    bucket: str,
    prefix: str = "cleaned",
    base_name: str = "ecommerce_latest",
    file_format: str = "csv"
) -> dict:
    """Saves file with a fixed name (overwrites). Useful for 'latest' version.

    Args:
        df: DataFrame to save.
        bucket: S3 bucket name.
        prefix: S3 folder prefix.
        base_name: Filename (without extension).
        file_format: 'csv' or 'parquet'.

    Returns:
        dict: Information about the saved file.
    """
    filename = f"{base_name}.{file_format}"
    key = f"{prefix}/{filename}"
    
    return save_dataframe_to_s3(df, bucket, key, file_format)


def verify_s3_file(bucket: str, key: str) -> dict:
    """Verifies that a file exists in S3 and retrieves its metadata.

    Args:
        bucket: S3 bucket name.
        key: File key.

    Returns:
        dict: File metadata.
    """
    s3 = get_s3_client()
    
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        
        return {
            "exists": True,
            "size_mb": response['ContentLength'] / (1024 * 1024),
            "last_modified": response['LastModified'].isoformat(),
            "metadata": response.get('Metadata', {})
        }
    except Exception as e:
        return {
            "exists": False,
            "error": str(e)
        }


def list_cleaned_files(bucket: str, prefix: str = "cleaned/") -> list:
    """Lists all files in the 'cleaned/' prefix.

    Args:
        bucket: S3 bucket name.
        prefix: Prefix to search.

    Returns:
        list: List of found files with metadata.
    """
    s3 = get_s3_client()
    
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            print(f"ℹ️  No files found in s3://{bucket}/{prefix}")
            return []
        
        files = []
        for obj in response['Contents']:
            files.append({
                'key': obj['Key'],
                'size_mb': obj['Size'] / (1024 * 1024),
                'last_modified': obj['LastModified'].isoformat()
            })
        
        return files
        
    except Exception as e:
        print(f"❌ Error listing files: {str(e)}")
        return []