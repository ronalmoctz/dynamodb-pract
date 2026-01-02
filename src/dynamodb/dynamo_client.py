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

def get_table(table_name: str = "Ecommerce_eu"):
    """Returns the DynamoDB table resource."""
    dynamodb = get_dynamodb_resource()
    return dynamodb.Table(table_name)