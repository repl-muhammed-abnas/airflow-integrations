from capgemini_test.time_export.config  import *

instance = "trial"

environment = "pre-prod"
replicon_conn_id = 'capgeminitest_repliconint_timeexport'

sftp_conn_id = "sftp_useast2"

file_format = "Time entry export"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
can_run_batch_task_var_name = f'capgemini_test_time_export_can_run_batch_task_{instance}'

export_file_prefix = "trial"
execution_timeout_days = 14