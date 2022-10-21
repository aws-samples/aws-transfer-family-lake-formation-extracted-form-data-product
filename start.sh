#!/bin/bash

# Installs Packages
sudo yum install jq -y
sudo yum install gnupg -y &&

# Generates a Key
echo "Creating GPG keys for encryption"
sudo gpg --batch --gen-key <<EOF
Key-Type: 1
Key-Length: 2048
Subkey-Type: 1
Subkey-Length: 2048
Name-Real: SFTPUser
Name-Email: SFTPUser@example.com
Expire-Date: 0
%no-protection
EOF

# Exports Variables from CloudFormation
# We will change the STACK_NAME variable to use the module ID once we have it
export STACK_NAME=TF-reInvent-2022
export EXTRACTED_BUCKET=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="ExtractedDataS3BucketName") | .OutputValue'`
export ZIPCODE_BUCKET=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="ZipcodeDataS3BucketName") | .OutputValue'`
export SECRET_ARN=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="PGPSecretARN") | .OutputValue'`

# Put Private Key in Secrets Manager
echo "Uploading GPG Private Key to AWS Secrets Manager"
touch private.key
sudo gpg --export-secret-key -a SFTPUser > private.key
aws secretsmanager put-secret-value --secret-id $SECRET_ARN --secret-string file://private.key

# Recursive Copy Data to S3 Buckets
echo "Uploading pre-staged data to S3"
aws s3 cp extracted/ s3://$EXTRACTED_BUCKET/ --recursive
aws s3 cp zipcode/ s3://$ZIPCODE_BUCKET/ --recursive

exit 0
