# src/data_load.py

import io
import os
import boto3
import pandas as pd
from pathlib import Path
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION


def get_s3_client():
    """Creates and returns a configured S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


def download_from_s3_to_cache(bucket: str, key: str, cache_dir: str = "data/cache") -> str:
    """Downloads a file from S3 and saves it to a local cache.

    Args:
        bucket: S3 bucket name.
        key: S3 object key.
        cache_dir: Local directory for caching.

    Returns:
        Path to the local cached file.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    
    # Create local filename based on S3 key
    local_filename = key.replace("/", "_")
    local_file_path = cache_path / local_filename
    
    # Check if exists in cache
    if local_file_path.exists():
        print(f"File found in cache: {local_file_path}")
        return str(local_file_path)
    
    # Download from S3 if not in cache
    print(f"Downloading from S3: s3://{bucket}/{key}")
    s3 = get_s3_client()
    s3.download_file(bucket, key, str(local_file_path))
    
    file_size_mb = local_file_path.stat().st_size / (1024 * 1024)
    print(f"Downloaded to cache: {local_file_path} ({file_size_mb:.2f} MB)")
    
    return str(local_file_path)


def load_data_from_s3(
    bucket: str, 
    key: str, 
    use_cache: bool = True,
    force_download: bool = False
) -> pd.DataFrame:
    """Loads CSV from S3 into a DataFrame (with cache support).

    Args:
        bucket: S3 bucket name.
        key: S3 object key.
        use_cache: If True, uses local cache (recommended for development).
        force_download: If True, forces download even if exists in cache.

    Returns:
        pd.DataFrame: Loaded data.
    """
    try:
        if use_cache:
            # Option 1: Use local cache (faster)
            cache_path = download_from_s3_to_cache(bucket, key)
            
            # If force download is needed, implement logic here (omitted for safety logic currently)
            # if force_download: ...

            # Read from local file
            print(f"Reading from local cache...")
            df = pd.read_csv(cache_path, encoding="ISO-8859-1")
            
        else:
            # Option 2: Download directly to memory
            print(f"📥 Downloading directly to memory from S3...")
            s3 = get_s3_client()
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(
                io.TextIOWrapper(obj["Body"], encoding="ISO-8859-1")
            )
        
        print(f"Loaded: {len(df):,} rows, {len(df.columns)} columns")
        return df
        
    except Exception as e:
        print(f" Error loading data: {str(e)}")
        raise


def load_data_from_s3_chunked(
    bucket: str, 
    key: str, 
    chunksize: int = 1000,
    use_cache: bool = True
):
    """Loads CSV from S3 in chunks (for large files).

    Args:
        bucket: S3 bucket name.
        key: S3 object key.
        chunksize: Number of rows per chunk.
        use_cache: If True, uses local cache.

    Yields:
        pd.DataFrame: Chunk of the CSV.
    """
    try:
        if use_cache:
            cache_path = download_from_s3_to_cache(bucket, key)
            print(f"📖 Reading chunks from cache (chunksize={chunksize:,})...")
            chunks = pd.read_csv(
                cache_path, 
                encoding="ISO-8859-1",
                chunksize=chunksize
            )
        else:
            print(f"📥 Reading chunks directly from S3...")
            s3 = get_s3_client()
            obj = s3.get_object(Bucket=bucket, Key=key)
            chunks = pd.read_csv(
                io.TextIOWrapper(obj["Body"], encoding="ISO-8859-1"),
                chunksize=chunksize
            )
        
        chunk_num = 0
        for chunk in chunks:
            chunk_num += 1
            print(f"Chunk {chunk_num}: {len(chunk)} rows")
            yield chunk
            
    except Exception as e:
        print(f"Error loading data chunks: {str(e)}")
        raise


def clear_cache(cache_dir: str = "data/cache"):
    """Clears the cache directory.

    Args:
        cache_dir: Directory to clear.
    """
    cache_path = Path(cache_dir)
    if cache_path.exists():
        import shutil
        shutil.rmtree(cache_path)
        print(f"🗑️  Cache cleared: {cache_dir}")
    else:
        print(f" No cache to clear")