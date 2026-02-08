from helper_utilities.common_util import common_utilities
import json

def main():
    try:
        util=common_utilities()
        logger=util.get_logger(__file__)
        
        logger.info("Started generating run id")
        run_id=util.generate_run_id()
        logger.info(f"Finished generating run id : {run_id}")

        logger.info(f"Started fetching data from api")
        data_list=util.fetch_data_from_source('countries')
        logger.info(f"Finished fetching data from api")

        logger.info(f"Started creating stage table for countries")
        stg_table=util.create_ingestion_stage_table("countries",run_id)
        logger.info(f"Finished creating stage table for countries")

        logger.info(f"Started ingestting data from source to stage table   {stg_table}")
        for data in data_list:
            data=json.dumps(data)
            util.execute_stored_procedure(f"CALL stg.load_json_data_from_source_to_stage_table(p_stg_table=>'{stg_table}',p_data=>$${data}$$,p_run_id=>'{run_id}',p_sub_domain=>'countries');")
        logger.info(f"Finished ingestting data from source to stage table   {stg_table}")

        logger.info("Started loading data into reference table")
        util.execute_stored_procedure(f"CALL load_country_data_incremental(p_run_id=>'{run_id}')")
        logger.info("Finished loading data into reference table")
        
        logger.info("Started logging succesfull ingestion status")
        util.execute_stored_procedure(f"CALL log.load_ingestion_logs(p_sub_domain=> 'countries',p_status=>'success',p_run_id=>'{run_id}')")
        logger.info("Finished logging ingestion status")
    
    except Exception as e:
        error_msg=str(e)
        logger.info("Started logging failed ingestion status")
        util.execute_stored_procedure(f"CALL log.load_ingestion_logs(p_sub_domain=> 'countries',p_status=>'fail',p_run_id=>'{run_id}',p_error_message=>'{error_msg}')")
        logger.info("Finished logging failed status")
        logger.error(e)

if __name__ == "__main__":
    main()