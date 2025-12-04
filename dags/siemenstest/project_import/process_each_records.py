import rail
from siemenstest.project_import.utils import custom_methods, request_methods

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_dagid,
        description=f"Siemens test Project Import child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")
        
        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectdetails3",
            data=lambda dag_run: {
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": dag_run.conf["project_code"],
                        "parameterCorrelationId": null,
                    }
                ]
            },
            data_handler=custom_methods.parse_project_response,
        )


        if_project_exists = rail.IfOperator(
            task_id="if_project_exists",
            test=lambda: rail.result("get_project_details")
            and rail.result("get_project_details").get("uri"),
            no_task="create_projects",
            yes_task="if_any_project_updates",
        )

        create_projects = rail.RepliconServiceOperator(
            task_id="create_projects",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.create_project(dag_run)
        )

        if_any_project_updates = rail.IfOperator(
            task_id="if_any_project_updates",
            test=lambda dag_run: custom_methods.check_for_project_updates(dag_run),
            yes_task="update_projects",
            no_task="print_no_updates",
        )

        print_no_updates = rail.PythonOperator(
            task_id = "print_no_updates",
            python_callable= lambda: "No updates"
        )
        
    
        update_projects= rail.RepliconServiceOperator(
            task_id="update_projects",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.get_custom_field_update_request(dag_run)
        )

        write_project_update_success = rail.WriteLogOperator(
            task_id="write_project_update_success",
            log="{{ dag_run.conf['master_log'] }}",
            message="Project Updated Successfully",
            properties=lambda dag_run: {
                "projectname": dag_run.conf["project_name"],
                "projectcode": dag_run.conf["project_code"],
                "status":  "Success",
                "details": "Project Updated Successfully",
            },
        )

        write_project_success = rail.WriteLogOperator(
            task_id="write_project_success",
            log="{{ dag_run.conf['master_log'] }}",
            message="Project Created Successfully",
            properties=lambda dag_run: {
                "projectname": dag_run.conf["project_name"],
                "projectcode": dag_run.conf["project_code"],
                "status": "Success",
                "details": "Project Processed Successfully",
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log="{{ dag_run.conf['master_log'] }}",
            trigger_rule="one_failed",
            message="Project is not processed",
            severity="Error",
            properties=lambda dag_run: {
                "projectname": dag_run.conf["project_name"],
                "projectcode": dag_run.conf["project_code"],
                "status": "Error",
            },
        )


        get_project_details >> if_project_exists >> rail.Label("Yes") >>if_any_project_updates 
        if_project_exists >> rail.Label("No") >> create_projects >>write_project_success
        write_project_success >> catch_and_log_errors
        
        if_any_project_updates >> rail.Label("Yes") >> update_projects >> write_project_update_success
        write_project_update_success >> catch_and_log_errors
        
        if_any_project_updates >> rail.Label("No") >> print_no_updates
       
        return dag


rail.for_each_instance(create_child_dag)
