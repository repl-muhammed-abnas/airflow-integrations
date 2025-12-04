from datetime import timedelta
import rail
from abnas_test.user_import.utils import request_payload, custom_methods


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"Atark_User_import_master_{config.instance}",
        description=f'Atark User Import Master',
        company_key="Atark",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=3,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
    
        
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )
    
        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.sftp_archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}''',
            existing_filename=config.sftp_input_filepath +
            '''/{{ result("new_file_sensor") | file_name }}''',
        )
    
        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_file') }}",
            encoding='utf-8'
        )

        filter_task = rail.PythonOperator(
            task_id='filter_valid_emails',
            python_callable=lambda: custom_methods.filter_valid_emails(rail.result('parse_csv')),
            show_return_value_in_logs=False
        )

        create_csv_lines_input = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_input',
            source="{{ result('filter_task') }}",
            header=[
                'Employee_ID', 'First_Name', 'Last_Name', 'Preferred_Name', 'Email'
            ],
            row=request_payload.row_data_for_input_file,
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        trigger_child = rail.trigger_parallel_dagrun(
            task_id='process_each_rec',
            items=lambda: rail.result('filter_task'),
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id='process_each_records',
            conf=lambda item: {
                **item
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        new_file_sensor >> download_file >> archive_file >> parse_csv >> filter_task >> create_csv_lines_input >> trigger_child

    return dag

rail.for_each_instance(create_main_dag)





    

