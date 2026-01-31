import json
from helper_utilities.common_util import common_utilities

util=common_utilities()
logger=util.get_logger(__file__)
# data=util.fetch_data_from_source("countries")
logger.info("Started generating run id")
run_id=util.generate_run_id()
logger.info(f"Finished generating run id : {run_id}")
logger.info(f"Started creating stage table for countries")
stg_table=util.create_ingestion_stage_table("countries",run_id)
logger.info(f"Finished creating stage table for countries")


logger.info(f"Started ingestting data from source to stage table   {stg_table}")
execute_stored_procedure(f"CALL stg.load_json_data_from_source_to_stage_table('{stg_table}','countries');")
logger.info(f"Finished ingestting data from source to stage table   {stg_table}")



# logger=a.get_logger("test_log.log")
# logger.info("This is an info message")

# conn = psycopg2.connect(
#         host="localhost",
#         database="cricket_centre",
#         user="postgres",
#         password="tarmak007",
#         port=5432 # Default PostgreSQL port
#     )
# print("Connected to the PostgreSQL server.")

# conn.autocommit = True
# cursor=conn.cursor()
# cursor.execute("TRUNCATE TABLE stg.current_matches;")

# for record in data['data']:
#     json_data=json.dumps(record)
#     cursor.execute(f"CALL stg.load_json_data(data=>'{json_data}');")
# cursor.close()
# # conn.autocommit = True
# # cursor=conn.cursor()
# # cursor.execute(f'CALL stg.load_json_data(data=>"{data}");')

# with open('C:\\Users\\Admin\\AppData\\Local\\config\\config.json', 'r') as file:
#     config = json.load(file)
# print(config['host'])