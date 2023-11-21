#!/bin/bash
set -x

export ACCOUNT=`aws sts get-caller-identity --query 'Account' --output text`

export AWS_REGION=`aws configure get region`

aws glue update-database --name employee_tax_db --database-input '{"Name": "employee_tax_db", "CreateTableDefaultPermissions": []}'

aws lakeformation grant-permissions --catalog-id $ACCOUNT --principal DataLakePrincipalIdentifier="arn:aws:iam::$ACCOUNT:role/service-role/TransferLab-AWSGlueServiceRole" --resource '{"Database": {"CatalogId": "'$ACCOUNT'", "Name": "employee_tax_db"}}' --permissions ALL
aws lakeformation grant-permissions --catalog-id $ACCOUNT --principal DataLakePrincipalIdentifier="arn:aws:iam::$ACCOUNT:role/service-role/TransferLab-AWSGlueServiceRole" --resource '{"Table": {"CatalogId": "'$ACCOUNT'", "DatabaseName": "employee_tax_db", "Name":"irs_tax_info_by_zip_2019" }}' --permissions SELECT
aws lakeformation revoke-permissions --catalog-id $ACCOUNT --principal DataLakePrincipalIdentifier="IAM_ALLOWED_PRINCIPALS" --resource '{"Table": {"CatalogId": "'$ACCOUNT'", "DatabaseName": "employee_tax_db", "Name":"irs_tax_info_by_zip_2019" }}' --permissions ALL
aws lakeformation register-resource --resource-arn arn:aws:s3:::transferlab-processed-$ACCOUNT --use-service-linked-role --no-hybrid-access-enabled
aws lakeformation grant-permissions --catalog-id $ACCOUNT --principal DataLakePrincipalIdentifier="arn:aws:iam::$ACCOUNT:role/service-role/TransferLab-AWSGlueServiceRole" --resource '{"DataLocation": {"CatalogId": "'$ACCOUNT'", "ResourceArn": "arn:aws:s3:::transferlab-processed-'$ACCOUNT'" }}' --permissions DATA_LOCATION_ACCESS
