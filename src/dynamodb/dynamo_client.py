import boto3
import logging
from botocore.exceptions import ClientError
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

logger = logging.getLogger(__name__)

def get_dynamodb_resource():
    """Returns a configured DynamoDB resource."""
    return boto3.resource(
        "dynamodb",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def create_table_if_not_exists(table_name: str = "Ecommerce_eu"):
    """
    Creates the DynamoDB table if it doesn't already exist.
    Key Schema:
        PK: InvoiceNo (String)
    GSIs:
        CountryIndex: PK=Country, SK=InvoiceDate
        CustomerIndex: PK=CustomerID, SK=InvoiceDate
    """
    dynamodb = get_dynamodb_resource()
    
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'InvoiceNo', 'KeyType': 'HASH'}  # Partition key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'InvoiceNo', 'AttributeType': 'S'},
                {'AttributeName': 'Country', 'AttributeType': 'S'},
                {'AttributeName': 'InvoiceDate', 'AttributeType': 'S'},
                {'AttributeName': 'CustomerID', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'CountryIndex',
                    'KeySchema': [
                        {'AttributeName': 'Country', 'KeyType': 'HASH'},
                        {'AttributeName': 'InvoiceDate', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                },
                {
                    'IndexName': 'CustomerIndex',
                    'KeySchema': [
                        {'AttributeName': 'CustomerID', 'KeyType': 'HASH'},
                        {'AttributeName': 'InvoiceDate', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"Creating table {table_name}...")
        table.wait_until_exists()
        print(f"Table {table_name} created successfully.")
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table {table_name} already exists.")
            return dynamodb.Table(table_name)
        else:
            logger.error(f"Error creating table: {e}")
            raise