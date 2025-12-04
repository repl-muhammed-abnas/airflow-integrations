from conduenttest.proj_import.config import *

instance = 'trial'
environment = 'pre-prod'
company_key = 'keyinstance2'
replicon_conn_id = 'conduenttest_repliconint_projectimport'
sftp_conn_id = 'sftp_useast2'

input_filepath = '/abnas/project_import/input/'
archive_filepath = '/abnas/project_import/archive/'
log_filepath = '/abnas/project_import/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'conduent_project_import_master_{instance}'