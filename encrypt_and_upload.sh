#!/bin/bash

# Install lftp to perform non-interactive connection to AWS Transfer Family SFTP Server
sudo yum install lftp -y

# Set variables from CFN Output
# # We will change the STACK_NAME variable to use the module ID once we have it
export STACK_NAME=transferfamilyworkflow
export TRANSFER_ENDPOINT=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="TransferFamilyServerEndpoint") | .OutputValue'`
export TRANSFER_ENDPOINT+=.server.transfer.us-east-1.amazonaws.com

# Make directory for encrypted files
mkdir init/encrypted/

# Encrypt raw files using GPG key
echo "Encrypting .png W2 files for upload"
sudo gpg --encrypt-files -r SFTPUser@example.com init/raw/*.png
sleep 3

# Move encrypted files to encrypted directory
echo "Moving GPG encrypted files to encrypted directory"
mv init/raw/*.png.gpg init/encrypted/
sleep 3

# Prompt user for password for SFTP connection string
echo -n "Enter the password for SFTPUser:"
read -s SFTP_PASSWORD
echo

# Connect to SFTP server and SFTP multiple files to the AWS Transfer Family Server
echo "Initiating SFTP of encrypted files to AWS Transfer Family Server"
lftp sftp://SFTPUser:$SFTP_PASSWORD@$TRANSFER_ENDPOINT  -e "mput init/encrypted/*.png.gpg; bye"

exit 0
