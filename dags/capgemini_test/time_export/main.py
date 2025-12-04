import rail
from capgemini_test.time_export.tasks.time_d_export import time_data_export
from capgemini_test.time_export.utils import custom_methods
from datetime import timedelta
import pendulum
from airflow.models import Variable

def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"capgeminitest_time_export_{config.instance}",
        description="time export test for capgemini",
        company_key="keyinstance",
        replicon_conn_id=config.replicon_conn_id
    ) as dag:
        
        write_task = rail.PythonOperator(
            task_id = 'print_test',
            python_callable = lambda: "testing rail.result()"
        )
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_start_time'
        )
        
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_start_time',
            end_task='finish_time_export_batch_creation',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )
        
        # print_task = rail.PythonOperator(
        #     task_id = 'print_test',
        #     python_callable = lambda: "hello"
        # )
        
        # will throw a DuplicateTaskIdFound: Task id 'print_test' has already been added to the DAG
         
        get_time_download_script = rail.RepliconServiceOperator(
            task_id='get_time_download_script',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler = lambda response: custom_methods.get_uri_by_display_text(response, config.file_format)
            # response_filter = lambda response: custom_methods.get_uri_by_display_text(response.json(), config.file_format)
        )
        
        group_id = 'time_data_export'

        time_export_batch_start = time_data_export(
            group_id=group_id
        )
        
        
        finish_time_export_batch_creation = rail.EmptyOperator(
            task_id='finish_time_export_batch_creation'
        )
        
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish_time_export_batch_creation
        can_run_batch_task >> rail.Label('No') >> process_start_time
        
        process_start_time >> write_task  >> get_time_download_script >> time_export_batch_start >> finish_time_export_batch_creation
        # if i call write_task again and it will return Cycle detected in DAG: capgeminitest_time_export_trial. Faulty task: download_export

        
        return dag



rail.for_each_instance(create_master_dag)