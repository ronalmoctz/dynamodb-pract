#!/bin/bash

# Create DynamoDB table 'Ecommerce_eu'
# PK: InvoiceNo
# SK: StockCode
# GSI1: CountryIndex (PK=Country, SK=InvoiceDate)
# GSI2: CustomerIndex (PK=CustomerID, SK=InvoiceDate)

echo "Deleting table Ecommerce_eu if it exists..."
aws dynamodb delete-table --table-name Ecommerce_eu 2>/dev/null || true
echo "Waiting for deletion to complete..."
aws dynamodb wait table-not-exists --table-name Ecommerce_eu
echo "Creating table Ecommerce_eu..."

aws dynamodb create-table \
    --table-name Ecommerce_eu \
    --attribute-definitions \
        AttributeName=InvoiceNo,AttributeType=S \
        AttributeName=StockCode,AttributeType=S \
        AttributeName=Country,AttributeType=S \
        AttributeName=InvoiceDate,AttributeType=S \
        AttributeName=CustomerID,AttributeType=S \
    --key-schema \
        AttributeName=InvoiceNo,KeyType=HASH \
        AttributeName=StockCode,KeyType=RANGE \
    --global-secondary-indexes \
        "[
            {
                \"IndexName\": \"CountryIndex\",
                \"KeySchema\": [
                    {\"AttributeName\": \"Country\", \"KeyType\": \"HASH\"},
                    {\"AttributeName\": \"InvoiceDate\", \"KeyType\": \"RANGE\"}
                ],
                \"Projection\": {
                    \"ProjectionType\": \"ALL\"
                }
            },
            {
                \"IndexName\": \"CustomerIndex\",
                \"KeySchema\": [
                    {\"AttributeName\": \"CustomerID\", \"KeyType\": \"HASH\"},
                    {\"AttributeName\": \"InvoiceDate\", \"KeyType\": \"RANGE\"}
                ],
                \"Projection\": {
                    \"ProjectionType\": \"ALL\"
                }
            }
        ]" \
    --billing-mode PAY_PER_REQUEST

echo "Table Ecommerce_eu created successfully."
