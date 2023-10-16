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

# Store current AWS Region as variables
AWS_REGION=`aws configure get region`

# Exports Variables from CloudFormation
# We will change the STACK_NAME variable to use the module ID once we have it
export STACK_NAME=transferfamilyworkflow
export EXTRACTED_BUCKET=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="ExtractedDataS3BucketName") | .OutputValue'`
export ZIPCODE_BUCKET=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="ZipcodeDataS3BucketName") | .OutputValue'`
export SECRET_ARN=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="PGPSecretARN") | .OutputValue'`
export GLUE_ASSETS_BUCKET=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="GlueAssetsS3BucketName") | .OutputValue'`
export PROCESSED_BUCKET=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="ProcessedDataS3BucketName") | .OutputValue'`
export GLUE_SERVICE_ROLE_ARN=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="DataLakeGlueServiceRoleArn") | .OutputValue'`
export TRANSFER_REPORTS_ENDPOINT=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="TransferFamilyReportsServerEndpoint") | .OutputValue'`
export REPORTS_CONNECTOR=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="ReportsSFTPConnector") | .OutputValue'`
export REPORTS_BUCKET=`aws cloudformation describe-stacks | jq -r --arg STACK_NAME "$STACK_NAME" '.Stacks[] | select(.StackName==$STACK_NAME) | .Outputs[] | select(.OutputKey=="ReportsDataS3BucketName") | .OutputValue'`

# Read host key from reports server and update TrustedHostKeys parameter in AWS Transfer connector
export TRANSFER_REPORTS_ENDPOINT+=.server.transfer.$AWS_REGION.amazonaws.com
ssh-keyscan $TRANSFER_REPORTS_ENDPOINT > /tmp/keyscan.tmp
export HOST_KEY=`cat /tmp/keyscan.tmp | grep -o 'ssh-rsa.*'`
aws transfer update-connector --connector-id $REPORTS_CONNECTOR --sftp-config 'TrustedHostKeys='"$HOST_KEY"''
# Create an empty reports folder in the reports bucket so that the folder appears if directories are listed on the reports SFTP server
aws s3api put-object --bucket $REPORTS_BUCKET --key reports/ 

# Put Private Key into Secrets Manager
echo "Uploading GPG Private Key to AWS Secrets Manager"
private_key=`sudo gpg --export-secret-key -a SFTPUser`
aws secretsmanager put-secret-value --secret-id $SECRET_ARN --secret-string '{"PGPPrivateKey" : "'"$private_key"'"}'


# Recursive Copy Data to S3 Buckets
echo "Uploading pre-staged data to S3"
aws s3 cp init/extracted/ s3://$EXTRACTED_BUCKET/ --recursive
aws s3 cp init/zipcode/ s3://$ZIPCODE_BUCKET/ --recursive

# Copy Glue job scripts to s3 bucket
aws s3 cp init/glue/W2_ETL_Job.py s3://$GLUE_ASSETS_BUCKET/scripts/W2_ETL_Job.py
aws s3 cp init/glue/Report.py s3://$GLUE_ASSETS_BUCKET/scripts/Report.py

# Copy report HTML into reports bucket
aws s3 cp init/reportsite/ s3://$REPORTS_BUCKET/ --recursive

# Create glue job
aws glue delete-job --job-name W2_ETL_Job
cat <<EOF > /tmp/glue-job-definition.json

{
        "Name": "W2_ETL_Job", 
        "Description": "",
        "Role": "$GLUE_SERVICE_ROLE_ARN",
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1
        },
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": "s3://$GLUE_ASSETS_BUCKET/scripts/W2_ETL_Job.py",
            "PythonVersion": "3"
        },
        "DefaultArguments": {
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://$GLUE_ASSETS_BUCKET/sparkHistoryLogs/",
            "--enable-job-insights": "true",
            "--enable-glue-datacatalog": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python",
            "--TempDir": "s3://$GLUE_ASSETS_BUCKET/temporary/"
        },
        "MaxRetries": 0,
        "Timeout": 5,
        "WorkerType": "G.1X",
        "NumberOfWorkers": 10,
        "GlueVersion": "3.0",
        "CodeGenConfigurationNodes": {
            "node-1668430793719": {
                "S3GlueParquetTarget": {
                    "Name": "Amazon S3",
                    "Inputs": [
                        "node-1668430772959"
                    ],
                    "PartitionKeys": [
                        [
                            "employee_tax_state"
                        ],
                        [
                            "employee_agi_stub"
                        ]
                    ],
                    "Path": "s3://$PROCESSED_BUCKET",
                    "Compression": "snappy",
                    "SchemaChangePolicy": {
                        "EnableUpdateCatalog": true,
                        "UpdateBehavior": "UPDATE_IN_DATABASE",
                        "Table": "transformed_w2_data",
                        "Database": "employee_tax_db"
                    }
                }
            },
            "node-1668430720949": {
                "CatalogSource": {
                    "Name": "AWS Glue Data Catalog",
                    "Database": "employee_tax_db",
                    "Table": "irs_tax_info_by_zip_2019"
                }
            },
            "node-1668430731414": {
                "Join": {
                    "Name": "Join",
                    "Inputs": [
                        "node-1668430720949",
                        "node-1668430714890"
                    ],
                    "JoinType": "right",
                    "Columns": [
                        {
                            "From": "node-1668430720949",
                            "Keys": [
                                [
                                    "zip_code"
                                ],
                                [
                                    "agi_stub"
                                ]
                            ]
                        },
                        {
                            "From": "node-1668430714890",
                            "Keys": [
                                [
                                    "employee_zip_code"
                                ],
                                [
                                    "employee_agi_stub"
                                ]
                            ]
                        }
                    ]
                }
            },
            "node-1668430714890": {
                "SelectFromCollection": {
                    "Name": "Select From Collection",
                    "Inputs": [
                        "node-1668430654057"
                    ],
                    "Index": 0
                }
            },
            "node-1668430772959": {
                "DropFields": {
                    "Name": "Drop Fields",
                    "Inputs": [
                        "node-1668430731414"
                    ],
                    "Paths": [
                        [
                            "state_fips"
                        ],
                        [
                            "num_of_individuals"
                        ],
                        [
                            "num_of_returns"
                        ],
                        [
                            "num_of_returns_with_total_income"
                        ],
                        [
                            "num_of_returns_with_salary_wages"
                        ]
                    ]
                }
            },
            "node-1668430577437": {
                "ApplyMapping": {
                    "Name": "Change Schema (Apply Mapping)",
                    "Inputs": [
                        "node-1668430548031"
                    ],
                    "Mapping": [
                        {
                            "ToKey": "2Federalincometaxwithheld",
                            "FromPath": [
                                "2Federalincometaxwithheld"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": true
                        },
                        {
                            "ToKey": "6Medicaretaxwithheld",
                            "FromPath": [
                                "6Medicaretaxwithheld"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": true
                        },
                        {
                            "ToKey": "employee_total_wages",
                            "FromPath": [
                                "1Wagestipsothercompensation"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": false
                        },
                        {
                            "ToKey": "3Socialsecuritywages",
                            "FromPath": [
                                "3Socialsecuritywages"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": true
                        },
                        {
                            "ToKey": "5Medicarewagesandtips",
                            "FromPath": [
                                "5Medicarewagesandtips"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": true
                        },
                        {
                            "ToKey": "4Socialsecuritytaxwithheld",
                            "FromPath": [
                                "4Socialsecuritytaxwithheld"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": true
                        },
                        {
                            "ToKey": "Lastname",
                            "FromPath": [
                                "Lastname"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": true
                        },
                        {
                            "ToKey": "employee_socialsecurity_number",
                            "FromPath": [
                                "aEmployeessocialsecuritynumber"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": false
                        },
                        {
                            "ToKey": "17Stateincometax",
                            "FromPath": [
                                "17Stateincometax"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": true
                        },
                        {
                            "ToKey": "bEmployeridentificationnumberEIN",
                            "FromPath": [
                                "bEmployeridentificationnumberEIN"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": true
                        },
                        {
                            "ToKey": "Thirdpartysickpay",
                            "FromPath": [
                                "Thirdpartysickpay"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": true
                        },
                        {
                            "ToKey": "Retirementplan",
                            "FromPath": [
                                "Retirementplan"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": true
                        },
                        {
                            "ToKey": "EmployersstateIDnumber",
                            "FromPath": [
                                "EmployersstateIDnumber"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": true
                        },
                        {
                            "ToKey": "Statutoryemployee",
                            "FromPath": [
                                "Statutoryemployee"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": true
                        },
                        {
                            "ToKey": "employee_tax_state",
                            "FromPath": [
                                "State"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": false
                        },
                        {
                            "ToKey": "Employeesfirstnameandinitial",
                            "FromPath": [
                                "Employeesfirstnameandinitial"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": true
                        },
                        {
                            "ToKey": "employers_name_address",
                            "FromPath": [
                                "cEmployersnameaddressandZIPcode"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": false
                        },
                        {
                            "ToKey": "16Statewagestipsetc",
                            "FromPath": [
                                "16Statewagestipsetc"
                            ],
                            "FromType": "double",
                            "ToType": "double",
                            "Dropped": true
                        },
                        {
                            "ToKey": "employee_address",
                            "FromPath": [
                                "fEmployeesaddressandZIPcode"
                            ],
                            "FromType": "string",
                            "ToType": "string",
                            "Dropped": false
                        }
                    ]
                }
            },
            "node-1668430548031": {
                "S3JsonSource": {
                    "Name": "Amazon S3",
                    "Paths": [
                        "s3://$EXTRACTED_BUCKET"
                    ],
                    "Exclusions": [],
                    "Recurse": true,
                    "AdditionalOptions": {
                        "EnableSamplePath": false,
                        "SamplePath": "s3://$EXTRACTED_BUCKET/Aaron_Lote_336.json"
                    },
                    "JsonPath": "",
                    "Multiline": false,
                    "OutputSchemas": [
                        {
                            "Columns": [
                                {
                                    "Name": "2Federalincometaxwithheld",
                                    "Type": "double"
                                },
                                {
                                    "Name": "6Medicaretaxwithheld",
                                    "Type": "double"
                                },
                                {
                                    "Name": "1Wagestipsothercompensation",
                                    "Type": "double"
                                },
                                {
                                    "Name": "3Socialsecuritywages",
                                    "Type": "double"
                                },
                                {
                                    "Name": "5Medicarewagesandtips",
                                    "Type": "double"
                                },
                                {
                                    "Name": "4Socialsecuritytaxwithheld",
                                    "Type": "double"
                                },
                                {
                                    "Name": "Lastname",
                                    "Type": "string"
                                },
                                {
                                    "Name": "aEmployeessocialsecuritynumber",
                                    "Type": "string"
                                },
                                {
                                    "Name": "17Stateincometax",
                                    "Type": "double"
                                },
                                {
                                    "Name": "bEmployeridentificationnumberEIN",
                                    "Type": "string"
                                },
                                {
                                    "Name": "Thirdpartysickpay",
                                    "Type": "string"
                                },
                                {
                                    "Name": "Retirementplan",
                                    "Type": "string"
                                },
                                {
                                    "Name": "EmployersstateIDnumber",
                                    "Type": "string"
                                },
                                {
                                    "Name": "Statutoryemployee",
                                    "Type": "string"
                                },
                                {
                                    "Name": "State",
                                    "Type": "string"
                                },
                                {
                                    "Name": "Employeesfirstnameandinitial",
                                    "Type": "string"
                                },
                                {
                                    "Name": "cEmployersnameaddressandZIPcode",
                                    "Type": "string"
                                },
                                {
                                    "Name": "16Statewagestipsetc",
                                    "Type": "double"
                                },
                                {
                                    "Name": "fEmployeesaddressandZIPcode",
                                    "Type": "string"
                                }
                            ]
                        }
                    ]
                }
            },
            "node-1668430654057": {
                "CustomCode": {
                    "Name": "Custom Transform",
                    "Inputs": [
                        "node-1668430577437"
                    ],
                    "Code": "import re\nimport json\nimport boto3\n# convert python function to pyspark UDF\nfrom pyspark.sql.functions import udf   \n\ndf = dfc.select(list(dfc.keys())[0]).toDF()\n\n# Size of adjusted gross income\ndef calculate_agi(income):\n    # AGI Stub will be calculated on the following logic\n    # 1 = $1 under $25,000\n    # 2 = $25,000 under $50,000\n    # 3 = $50,000 under $75,000\n    # 4 = $75,000 under $100,000\n    # 5 = $100,000 under $200,000\n    # 6 = $200,000 or more\n    agi_stub = 0\n    \n    if income < 25000:\n        agi_stub = 1\n    elif income < 50000:\n        agi_stub = 2\n    elif income < 75000:\n        agi_stub = 3\n    elif income < 100000:\n        agi_stub = 4\n    elif income < 200000:\n        agi_stub = 5\n    else:\n        agi_stub = 6\n        \n    return agi_stub\n\n# extract address portion from the address\ndef extract_zip(address):\n    zipCode = \"xxxxx\"\n    if(address is not None):\n        print(f\"incoming address: {address}\")\n        match = re.search(r'.*(\\\\d{5}(\\\\-\\\\d{4})?)$', address)\n        if match:\n            zipCode = match.groups()[0]\n    return zipCode\n\n# extract employer name from the employer address\ndef extract_company_name(address):\n    companyName = \"abcd\"\n    if(address is not None):\n        splits = address.split(\",\", 1)\n        if(len(splits) > 0): \n            companyName = splits[0]\n    return companyName\n\n# extract zip code from address\naddress_parser_udf = udf(extract_zip)\ndf1 = df.withColumn('employer_zip_code', address_parser_udf('employers_name_address'))\ndf2 = df1.withColumn('employee_zip_code', address_parser_udf('employee_address'))\n\n# calculate the tax slab from the total wages\ncalculate_agi_udf = udf(calculate_agi)\n# This logic is probably incorrect when applied for taxation purpose\n# Done only for demo purpose    \ndf3 = df2.withColumn('employee_agi_stub', calculate_agi_udf('employee_total_wages'))\n\n# extract the employer name\nextract_comp_name_udf = udf(extract_company_name)\ndf4 = df3.withColumn('employer_name', extract_comp_name_udf('employers_name_address'))\n   \nnew_df = DynamicFrame.fromDF(df4, glueContext, \"newW2withTransformedData\")\n\n# Purge existing processed dataset\nsts = boto3.client(\"sts\")\naccount_id = sts.get_caller_identity()[\"Account\"]\nglueContext.purge_s3_path(f\"s3://transferlab-processed-{account_id}/\", {\"retentionPeriod\": 0})\n\nreturn(DynamicFrameCollection({\"CustomTransform0\": new_df}, glueContext))\n",
                    "ClassName": "MyTransform",
                    "OutputSchemas": [
                        {
                            "Columns": [
                                {
                                    "Name": "employee_zip_code",
                                    "Type": "string"
                                },
                                {
                                    "Name": "employer_name",
                                    "Type": "string"
                                },
                                {
                                    "Name": "employer_zip_code",
                                    "Type": "string"
                                },
                                {
                                    "Name": "employee_agi_stub",
                                    "Type": "string"
                                },
                                {
                                    "Name": "employee_total_wages",
                                    "Type": "double"
                                },
                                {
                                    "Name": "employee_socialsecurity_number",
                                    "Type": "string"
                                },
                                {
                                    "Name": "employee_tax_state",
                                    "Type": "string"
                                },
                                {
                                    "Name": "employers_name_address",
                                    "Type": "string"
                                },
                                {
                                    "Name": "employee_address",
                                    "Type": "string"
                                }
                            ]
                        }
                    ]
                }
            }
        }
}

EOF

aws glue create-job --cli-input-json file:///tmp/glue-job-definition.json

exit 0
