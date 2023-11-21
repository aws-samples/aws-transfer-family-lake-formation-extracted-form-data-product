import argparse
import json
import logging
import re
import sys

import awswrangler as wr
import pandas

log = logging.getLogger("report-generator")
log.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

arg_parser = argparse.ArgumentParser(
    description="Executes an Athena query and writes the results to an S3 bucket.", 
    epilog="""
    """
)

arg_parser.add_argument('--debug-logging', action='store_true', required=False, default=False, help="When set, debug level logging will be enabled")
arg_parser.add_argument('--result-bucket', required=True, help="The S3 bucket to send the report to.")
arg_parser.add_argument('--result-path', required=True, help="The path, including filename, to send the report to (e.g. /path/to/report.html).")
arg_parser.add_argument('--workgroup', required=True, help="The Athena workgroup to use for the query.")
arg_parser.add_argument('--database', required=True, help="The glue database used to execute the query.")

args = vars(arg_parser.parse_known_args()[0])
if args['debug_logging']: 
    handler.setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
log.addHandler(handler)

log.debug({i:args[i] for i in args})

RESULT_BUCKET = args['result_bucket']
RESULT_PATH = args['result_path']
DATABASE = args['database']
WORKGROUP = args['workgroup']

ATHENA_SQL = """
		SELECT employee_zip_code,
			avg(employee_total_wages) as average_wages,
			min(employee_total_wages) as min_wages,
			max (employee_total_wages) as max_wages,
			avg(adjusted_gross_income) as average_agi,
			min(adjusted_gross_income) as min_agi,
			max(adjusted_gross_income) as max_agi
		FROM "employee_tax_db"."transformed_w2_data"
		GROUP BY employee_zip_code
"""

# Use AWS SDK for pandas to execute the Athena SQL query into a pandas dataframe
log.info("Executing Athena query")
log.debug(ATHENA_SQL)
df = wr.athena.read_sql_query(sql=ATHENA_SQL, database=DATABASE, ctas_approach=False, workgroup=WORKGROUP)

# Use AWS SDK for pandas to write the pandas dataframe to an HTML File
log.info(f"Writing results to s3://{RESULT_BUCKET}{RESULT_PATH}")
wr.s3.to_csv(df=df, path=f"s3://{RESULT_BUCKET}{RESULT_PATH}")


    