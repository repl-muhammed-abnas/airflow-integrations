import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"process_each_records_{config.instance}",
        description='atark user import',
        company_key="Atark",
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    return dag

rail.for_each_instance(create_child_dag)

