#!/bin/bash
set -x

export ACCOUNT=`aws sts get-caller-identity --query 'Account' --output text`

export AWS_REGION=`aws configure get region`

aws lakeformation grant-permissions --catalog-id $ACCOUNT --principal DataLakePrincipalIdentifier="arn:aws:iam::$ACCOUNT:role/TransferLab-LF-ColumnConstraint" --resource '{"TableWithColumns": {"CatalogId": "'$ACCOUNT'", "DatabaseName": "employee_tax_db", "Name":"transformed_w2_data", "ColumnWildcard": {"ExcludedColumnNames": ["employee_socialsecurity_number"]} }}' --permissions SELECT
aws lakeformation delete-data-cells-filter --table-catalog-id $ACCOUNT --database-name employee_tax_db --table-name transformed_w2_data --name row-filter
aws lakeformation create-data-cells-filter --table-data '{"TableCatalogId": "'$ACCOUNT'", "DatabaseName": "employee_tax_db", "TableName": "transformed_w2_data", "Name": "row-filter", "RowFilter": { "FilterExpression": "employer_name = '"'"'ExampleCorp Technology Inc'"'"'"}, "ColumnWildcard": {}}'
aws lakeformation grant-permissions --catalog-id $ACCOUNT --principal DataLakePrincipalIdentifier="arn:aws:iam::$ACCOUNT:role/TransferLab-LF-RowConstraint" --resource '{"DataCellsFilter": {"TableCatalogId": "'$ACCOUNT'", "DatabaseName": "employee_tax_db", "TableName":"transformed_w2_data", "Name": "row-filter"}}' --permissions SELECT


