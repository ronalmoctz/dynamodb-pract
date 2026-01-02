from config import S3_BUCKET_NAME, S3_RAW_DATA

# Data cleaning functions
from data_cleaning import (
    clean_columns,
    clean_dates,
    clean_numeric,
    remove_invalid_sales,
    add_total_amount
)

# Data loading functions
from data_load import load_data_from_s3, clear_cache

# Visualization functions
from utils.tablate import (
    print_data_summary,
    print_full_report
)

# Data saving functions
from data_save import save_with_timestamp, save_latest, verify_s3_file, list_cleaned_files

# Validate required configuration
if not S3_BUCKET_NAME or not S3_RAW_DATA:
    raise ValueError("S3_BUCKET_NAME or S3_RAW_DATA are not configured. Check your .env and config.py")


def clean_data(df):
    """Applies the data cleaning pipeline.

    Args:
        df: Input DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    print("\n📊 Starting data cleaning pipeline...")
    
    df = clean_columns(df)
    print("✅ Columns normalized")
    
    df = clean_dates(df, "invoicedate")
    print("✅ Dates converted")
    
    df = clean_numeric(df, ["quantity", "unitprice"])
    print("✅ Numeric values converted")
    
    df = remove_invalid_sales(df)
    print("✅ Invalid sales removed")
    
    df = add_total_amount(df)
    print("✅ 'total_amount' calculated")
    
    return df


def get_data_summary(df):
    """Generates a minimal hard data summary of the DataFrame."""
    total_rows = len(df)
    total_cols = len(df.columns)
    memory_mb = df.memory_usage(deep=True).sum() / 1024**2
    nulls = df.isnull().sum().to_dict()
    dup_count = int(df.duplicated().sum())

    return {
        'rows': total_rows,
        'cols': total_cols,
        'memory_mb': memory_mb,
        'nulls': nulls,
        'duplicates': dup_count
    }


def main(
    use_cache: bool = True,
    force_download: bool = False,
    save_cleaned: bool = True,
    save_format: str = "csv",
    timestamped: bool = False
):
    """Main data loading and cleaning pipeline.

    Args:
        use_cache: If True, uses local cache (recommended for development).
        force_download: If True, forces download even if exists in cache.
        save_cleaned: If True, saves the cleaned DataFrame to S3.
        save_format: Format to save ('csv'|'parquet').
        timestamped: If True, saves with timestamp; otherwise overwrites 'latest'.
    """
    print(f"\n{'='*60}")
    print("🚀 Starting E-commerce Data Pipeline")
    print(f"{'='*60}")
    print(f"📦 S3 Bucket: {S3_BUCKET_NAME}")
    print(f"📄 S3 Key: {S3_RAW_DATA}")
    print(f"💾 Use Cache: {use_cache}")
    
    # Load data from S3
    df = load_data_from_s3(
        bucket=S3_BUCKET_NAME,
        key=S3_RAW_DATA,
        use_cache=use_cache,
        force_download=force_download
    )
    
    print(f"\n📋 Original Data: {len(df):,} rows, {len(df.columns)} columns")
    
    # Clean data
    df = clean_data(df)
    
    print(f"\n✨ Final Data: {len(df):,} rows, {len(df.columns)} columns")
    print(f"\n📌 Columns: {df.columns.tolist()}")
    
    # ========================================
    # STEP 2: DATA TRANSFORMATION REPORT
    # ========================================
    print("\n🧹 STEP 2: DATA TRANSFORMATION REPORT")
    print("-"*80)

    # Generate and show summary
    summary = get_data_summary(df)
    print_data_summary(summary)

    # Show compact visual report
    print_full_report(df)

    # ========================================
    # STEP 3: LOAD (Save to S3)
    # ========================================
    if save_cleaned:
        print("\n💾 STEP 3: UPLOADING CLEANED DATA")
        print("-"*80)

        result_timestamped = {}
        result_latest = {}

        # Option A: Save with timestamp (history)
        if timestamped:
            try:
                result_timestamped = save_with_timestamp(
                    df=df,
                    bucket=S3_BUCKET_NAME,
                    prefix="cleaned",
                    base_name="ecommerce_europa",
                    file_format=save_format
                )
            except Exception as e:
                result_timestamped = {"success": False, "error": str(e)}

        # Option B: Save "latest" version (always latest)
        try:
            result_latest = save_latest(
                df=df,
                bucket=S3_BUCKET_NAME,
                prefix="cleaned",
                base_name="ecommerce_europa_latest",
                file_format=save_format
            )
        except Exception as e:
            result_latest = {"success": False, "error": str(e)}

        # ========================================
        # STEP 4: VERIFICATION
        # ========================================
        print("\n🔍 STEP 4: VERIFICATION")
        print("-"*80)

        if result_latest.get("success") and result_latest.get("s3_uri"):
            key = result_latest["s3_uri"].replace(f"s3://{S3_BUCKET_NAME}/", "")
            try:
                verification = verify_s3_file(bucket=S3_BUCKET_NAME, key=key)
                if verification.get("exists"):
                    print(f"✅ File verified in S3")
                    print(f"   Size: {verification['size_mb']:.2f} MB")
                    print(f"   Last Modified: {verification['last_modified']}")
                else:
                    print(f"⚠️  Could not verify file in S3: {verification.get('error')}")
            except Exception as e:
                print(f"⚠️  Error verifying in S3: {e}")
        else:
            print("⚠️  'Latest' version was not saved correctly; verification skipped.")

        # List all cleaned files
        print("\n📁 Files in 'cleaned/' folder:")
        try:
            files = list_cleaned_files(S3_BUCKET_NAME, prefix="cleaned/")
            for f in files:
                print(f"   • {f['key']} ({f['size_mb']:.2f} MB)")
        except Exception as e:
            print(f"⚠️  Error listing files in S3: {e}")

        # ========================================
        # FINAL SUMMARY
        # ========================================
        print("\n" + "="*80)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"\n📊 Results:")
        print(f"   • Processed Rows: {len(df):,}")
        if timestamped:
            print(f"   • Timestamped File: {result_timestamped.get('s3_uri', 'N/A')}")
        print(f"   • Latest File: {result_latest.get('s3_uri', 'N/A')}")
        print("\n" + "="*80 + "\n")
    
    print(f"{'='*60}")
    print("✅ Pipeline finished")
    print(f"{'='*60}\n")
    
    return df


if __name__ == "__main__":
    # Execute pipeline with cache enabled (development)
    # Set use_cache=False for direct S3 reading
    main(use_cache=True, force_download=False, save_cleaned=True, save_format='csv', timestamped=False)
    
    # Optional: clear cache afterwards
    # clear_cache()
