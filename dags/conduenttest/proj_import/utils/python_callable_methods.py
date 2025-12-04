import rail
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

null = None

MANDATORY_FIELDS = {
    "project_code": "Project ID",
    "project_status": "Project Status",
    "project_name": "Project Name",
    "est_cost": "Estimated Cost",
    "start_date": "Date Opened",
    "end_date": "Date Closed"
}


def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"Project creation/updation is skipped as the mandatory value {log_value} not present in the input file")
    return rail.smartjoin_by_delim(missing_fields, ";")


def _log_invalid_records(task_res):
    items = task_res or []
    logging.info("Invalid records count = %d", len(items))
    for idx, item in enumerate(items, start=1):
        logging.info("Invalid[%d]: %s", idx, item)
    return None
    
                

def _log_valid_records(task_res):
    items = task_res or []
    logging.info("Valid records count = %d", len(items))
    for idx, item in enumerate(items, start=1):
        logging.info("Valid[%d]: %s", idx, item)
    return None