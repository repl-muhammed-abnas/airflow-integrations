import rail
from datetime import timedelta


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Conduent test Master Project Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_input_csv = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv',
            remote_filepath="{{ result('new_file_sensor') }}",
        )
        
        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_input_csv') }}",
            encoding= 'utf-8'
        )

        create_collection_create_list_from_csv_raw_data = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_raw_data',
            source="{{ result('parse_csv') }}",
            name="inputfile",
            columns={
                'Project ID': 'project_code',
                'Project Status': 'project_status',
                'Project Name': 'project_name',
                'Estimated Cost': 'est_cost',
                'Date Opened': 'start_date',
                'Date Closed': 'end_date'
            }
        )
         
        if_any_records =  rail.IfOperator(
            task_id = "if_any_records",
            test="{{ result('create_collection_create_list_from_csv_raw_data','length') > 0 }}",
            yes_task="create_log_artifact", 
            no_task="send_norecords_mail"
        )

        create_log_artifact = rail.CreateLogOperator(
            task_id = "create_log_artifact"
        )
        
        query_list_projects_missing_required_fields = rail.QueryCollectionOperator(
            task_id='query_list_projects_missing_required_fields',
            name='invalidinputlist',
            query="""SELECT * FROM inputfile WHERE NULLIF(project_code,'') IS NULL 
            OR NULLIF(project_status,'') IS NULL OR NULLIF(project_name,'') IS NULL 
            OR NULLIF(start_date,'') IS NULL OR NULLIF(end_date,'') IS NULL 
            OR NULLIF(est_cost,'') IS NULL"""
        )
        
        
        if_query_list_projects_missing_required_fields_has_data = rail.IfOperator(
            task_id='if_query_list_projects_missing_required_fields_has_data',
            test="{{ result('query_list_projects_missing_required_fields','length') > 0 }}",
            yes_task="log_invalid_records", 
            no_task="query_list_projects_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id="log_invalid_records",
            log="{{ result('create_log_artifact')}}",
            severity="Exception",
            message="Invalid records",
            items="{{result('query_list_projects_missing_required_fields')}}",
            properties=lambda item:{
                "project_code": item["project_code"],
                "project_name": item["project_name"],
                "status": "Exception",
                "action": "validation"
            }
        )

        query_list_projects_valid_records = rail.QueryCollectionOperator(
            task_id='query_list_projects_valid_records',
            name="validatedinputlist",
            query="""SELECT * FROM inputfile WHERE NULLIF(project_code,'') IS NOT NULL 
            AND NULLIF(project_status,'') IS NOT NULL 
            AND NULLIF(project_name,'') IS NOT NULL 
            AND NULLIF(start_date,'') IS NOT NULL 
            AND NULLIF(end_date,'') IS NOT NULL 
            AND NULLIF(est_cost,'') IS NOT NULL"""
        )

        for_each_validrecords = rail.ForEachOperator(
            task_id="for_each_validrecords",
            start_task="empty_start",
            end_task="empty_end",
            items="{{result('query_list_projects_valid_records')}}"
        )
 

        empty_start = rail.EmptyOperator(task_id="empty_start")
 
        debug = rail.PythonOperator(
            task_id="debug",
            python_callable=lambda: rail.result("for_each_validrecords")
        )
 
        empty_end = rail.PythonOperator(
            task_id="empty_end",
            python_callable= lambda: "hello world"      
        )   

        new_file_sensor >> download_input_csv >> parse_csv >> create_collection_create_list_from_csv_raw_data >> if_any_records
        
        if_any_records >> rail.Label("Yes") >> create_log_artifact >> query_list_projects_missing_required_fields >> if_query_list_projects_missing_required_fields_has_data

        if_query_list_projects_missing_required_fields_has_data >> rail.Label("Yes") >> log_invalid_records >> query_list_projects_valid_records
        if_query_list_projects_missing_required_fields_has_data >> rail.Label("No")\
        >> query_list_projects_valid_records >> for_each_validrecords >> empty_end 
        
        for_each_validrecords >> empty_start >> debug >> empty_end 

        

    return dag

rail.for_each_instance(create_dag)
