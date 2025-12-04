from siemenstest.project_import.config import *

instance = "trial"
replicon_conn_id = "siemenstest_trial_repliconInt"
sftp_conn_id = "sftp_useast2"


input_filepath = "/abnas/project_import/input"
logs_filepath = "/abnas/project_import/logs"
input_archive_filepath = "/abnas/project_import/archive/"

master_dagid = f"siemenstest_project_import_master_{instance}"
process_project_dagid = f"siemenstest_project_import_process_each_records_child_{instance}"
