import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import re
    import json
    import boto3

    # convert python function to pyspark UDF
    from pyspark.sql.functions import udf

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Size of adjusted gross income
    def calculate_agi(income):
        # AGI Stub will be calculated on the following logic
        # 1 =  under 5,000
        # 2 = 5,000 under 0,000
        # 3 = 0,000 under 5,000
        # 4 = 5,000 under 00,000
        # 5 = 00,000 under 00,000
        # 6 = 00,000 or more
        agi_stub = 0

        if income < 25000:
            agi_stub = 1
        elif income < 50000:
            agi_stub = 2
        elif income < 75000:
            agi_stub = 3
        elif income < 100000:
            agi_stub = 4
        elif income < 200000:
            agi_stub = 5
        else:
            agi_stub = 6

        return agi_stub

    # extract address portion from the address
    def extract_zip(address):
        zipCode = "xxxxx"
        if address is not None:
            print(f"incoming address: {address}")
            match = re.search(r".*(\d{5}(\-\d{4})?)$", address)
            if match:
                zipCode = match.groups()[0]
        return zipCode

    # extract employer name from the employer address
    def extract_company_name(address):
        companyName = "abcd"
        if address is not None:
            splits = address.split(",", 1)
            if len(splits) > 0:
                companyName = splits[0]
        return companyName

    # extract zip code from address
    address_parser_udf = udf(extract_zip)
    df1 = df.withColumn(
        "employer_zip_code", address_parser_udf("employers_name_address")
    )
    df2 = df1.withColumn("employee_zip_code", address_parser_udf("employee_address"))

    # calculate the tax slab from the total wages
    calculate_agi_udf = udf(calculate_agi)
    # This logic is probably incorrect when applied for taxation purpose
    # Done only for demo purpose
    df3 = df2.withColumn("employee_agi_stub", calculate_agi_udf("employee_total_wages"))

    # extract the employer name
    extract_comp_name_udf = udf(extract_company_name)
    df4 = df3.withColumn(
        "employer_name", extract_comp_name_udf("employers_name_address")
    )

    new_df = DynamicFrame.fromDF(df4, glueContext, "newW2withTransformedData")

    # Purge existing processed dataset
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    glueContext.purge_s3_path(
        f"s3://transferlab-processed-{account_id}/", {"retentionPeriod": 0}
    )

    return DynamicFrameCollection({"CustomTransform0": new_df}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1668430720949 = glueContext.create_dynamic_frame.from_catalog(
    database="employee_tax_db",
    table_name="irs_tax_info_by_zip_2019",
    transformation_ctx="AWSGlueDataCatalog_node1668430720949",
)

# Script generated for node Amazon S3
AmazonS3_node1668430548031 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://transferlab-extracted-892043310681"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1668430548031",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1668430577437 = ApplyMapping.apply(
    frame=AmazonS3_node1668430548031,
    mappings=[
        ("1Wagestipsothercompensation", "double", "employee_total_wages", "double"),
        (
            "aEmployeessocialsecuritynumber",
            "string",
            "employee_socialsecurity_number",
            "string",
        ),
        ("State", "string", "employee_tax_state", "string"),
        (
            "cEmployersnameaddressandZIPcode",
            "string",
            "employers_name_address",
            "string",
        ),
        ("fEmployeesaddressandZIPcode", "string", "employee_address", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1668430577437",
)

# Script generated for node Custom Transform
CustomTransform_node1668430654057 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {
            "ChangeSchemaApplyMapping_node1668430577437": ChangeSchemaApplyMapping_node1668430577437
        },
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1668430714890 = SelectFromCollection.apply(
    dfc=CustomTransform_node1668430654057,
    key=list(CustomTransform_node1668430654057.keys())[0],
    transformation_ctx="SelectFromCollection_node1668430714890",
)

# Script generated for node Join
AWSGlueDataCatalog_node1668430720949DF = AWSGlueDataCatalog_node1668430720949.toDF()
SelectFromCollection_node1668430714890DF = SelectFromCollection_node1668430714890.toDF()
Join_node1668430731414 = DynamicFrame.fromDF(
    AWSGlueDataCatalog_node1668430720949DF.join(
        SelectFromCollection_node1668430714890DF,
        (
            AWSGlueDataCatalog_node1668430720949DF["zip_code"]
            == SelectFromCollection_node1668430714890DF["employee_zip_code"]
        )
        & (
            AWSGlueDataCatalog_node1668430720949DF["agi_stub"]
            == SelectFromCollection_node1668430714890DF["employee_agi_stub"]
        ),
        "right",
    ),
    glueContext,
    "Join_node1668430731414",
)

# Script generated for node Drop Fields
DropFields_node1668430772959 = DropFields.apply(
    frame=Join_node1668430731414,
    paths=[
        "state_fips",
        "num_of_individuals",
        "num_of_returns",
        "num_of_returns_with_total_income",
        "num_of_returns_with_salary_wages",
    ],
    transformation_ctx="DropFields_node1668430772959",
)

# Script generated for node Amazon S3
AmazonS3_node1668430793719 = glueContext.getSink(
    path="s3://transferlab-processed-892043310681",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["employee_tax_state", "employee_agi_stub"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1668430793719",
)
AmazonS3_node1668430793719.setCatalogInfo(
    catalogDatabase="employee_tax_db", catalogTableName="transformed_w2_data"
)
AmazonS3_node1668430793719.setFormat("glueparquet")
AmazonS3_node1668430793719.writeFrame(DropFields_node1668430772959)
job.commit()
