import logging
import re
from venv import logger
import os
from datetime import datetime
import json
import psycopg2
import requests
import uuid
from psycopg2 import sql
from psycopg2.extras import json

class common_utilities:
    def __init__(self):
        pass

    def read_config_file(self):
        try:
            with open('C:\\Users\\Admin\\AppData\\Local\\config\\config.json', 'r') as file:
                config = json.load(file)
            return config
        except Exception as e:
            raise Exception(f"Error reading config file: {e}")

    def get_logger(self,log_file_name,level=logging.INFO):
        class CustomFileHandler(logging.Handler):
            def __init__(self, filename):
                super().__init__()
                self.filename = filename

            def emit(self, record):
                # Format the record before writi
                year=datetime.now().strftime("%Y")
                month=datetime.now().strftime("%m") 
                day=datetime.now().strftime("%d")
                os.makedirs(os.getcwd()+f"\\logs\\{year}\\{month}\\{day}",exist_ok=True)
                
                msg = self.format(record)
                with open(f"{os.getcwd()}"+f"\\logs\\{year}\\{month}\\{day}\\{self.filename}", 'a+') as f:
                    f.write(f"{msg}\n")
        try:
            log_file_name=log_file_name.split('\\')[-1].replace('.py','.log') if '\\' in log_file_name else log_file_name.replace('.py','.log')
            logger = logging.getLogger(log_file_name)
            logger.setLevel(level)
            custom_handler = CustomFileHandler(log_file_name)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            custom_handler.setFormatter(formatter)
            logger.addHandler(custom_handler)
            return logger
        except Exception as e:
            raise Exception(f"Error in setting up logger: {e}")
        
    def establish_connection_with_db(self):
        try:
            config=self.read_config_file()
            conn = psycopg2.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            port=config['port'] # Default PostgreSQL port
            )
            return conn
        except Exception as e:
            raise Exception(f"Error reading config file: {e}")


    def fetch_data_from_source(self, sub_domain):
        try:
            config=self.read_config_file()
            apikey=config['apikey']
            offset=0
            max_offset=1
            data=[]
            url=f"https://api.cricapi.com/v1/{sub_domain}?apikey={apikey}"
            while offset<max_offset:
                print(max_offset)
                response=requests.get(f"{url}&offset={offset}")
                if response.status_code==200:
                    data.extend(response.json()['data'])
                    max_offset = response.json()['info']['totalRows']
                    offset+=len(response.json()['data'])
                    print(offset,max_offset)
                else:
                    raise Exception(f"Error fetching data from source: {response.status_code} - {response.text}")
            return data
        except Exception as e:
            raise Exception(e)
        
    def create_ingestion_stage_table(self,table_name,run_id):
        try:
            conn=self.establish_connection_with_db()
            cursor=conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS stg.{table_name};")
            cursor.execute(f"""CREATE TABLE stg.{table_name}_{run_id}(
                           value TEXT,
                           run_id VARCHAR(200)
                           );""")
            conn.commit()
            cursor.close()
            conn.close()
            return f"stg.{table_name}_{run_id}"
        except Exception as e:
            raise Exception(f"Error creating stage table: {e}")
        
    def generate_run_id(self):
        try:
            run_id=str(uuid.uuid4()).replace('-','_')
            return run_id
        except Exception as e:
            raise Exception(f"Error generating run id: {e}")
    
    def execute_stored_procedure(self,sp_query):
        try:

            # placeholders=sql.SQL(', ').join(sql.Placeholder() * len(params_list))
            # query=sql.SQL("CALL {}.{}({})").format(sql.Identifier(sp_schema),sql.Identifier(sp_name),placeholders)
            conn=self.establish_connection_with_db()
            cursor=conn.cursor()
            cursor.execute(sp_query)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(e)
            raise Exception(f"Error executing stored procedure: {e}")