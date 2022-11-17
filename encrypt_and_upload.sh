#!/bin/bash

# Install lftp to perform non-interactive connection to AWS Transfer Family SFTP Server
sudo yum install lftp -y &>/dev/null

# Store current AWS Region as variables
AWS_REGION=`aws configure get region`

# Set variables from CFN Output
export STACK_NAME=transferfamilyworkflow
export TRANSFER_ENDPOINT=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="TransferFamilyServerEndpoint") | .OutputValue'`
export TRANSFER_ENDPOINT+=.server.transfer.$AWS_REGION.amazonaws.com
export SFTPUSER_SECRETARN=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="SFTPUserSecretARN") | .OutputValue'`

# Store SFTPUser Password from Secret as variable
SFTP_SECRET=`aws secretsmanager get-secret-value --secret-id $SFTPUSER_SECRETARN`
export SFTP_PASSWORD=`echo $SFTP_SECRET | jq -r '.SecretString' | jq -r '.Password'`

# Make directory for encrypted files
mkdir init/encrypted/

# Encrypt raw files using GPG key
echo "Encrypting .png W2 files for upload"
sudo gpg --encrypt-files -r SFTPUser@example.com init/raw/*.png &>/dev/null
sleep 3

# Move encrypted files to encrypted directory
echo "Moving GPG encrypted files to encrypted directory"
mv init/raw/*.png.gpg init/encrypted/
sleep 3

# Connect to SFTP server and SFTP multiple files to the AWS Transfer Family Server
echo "Initiating SFTP of encrypted files to AWS Transfer Family Server"
lftp sftp://SFTPUser:$SFTP_PASSWORD@$TRANSFER_ENDPOINT  -e "mput init/encrypted/*.png.gpg; bye"
echo "SFTP of encrypted files complete"

exit 0
