import rail

null = None

def parse_project_response(response):
    if not response or not response[0].get("projectDetails"):
        return null

    project = response[0]["projectDetails"]

    return {
        "uri": project.get("uri"),
        "start_date":project.get("timeEntryDateRange",{}).get("startDate"),
        "end_date":project.get("timeEntryDateRange",{}).get("endDate"),
        "name": project.get("name"),
        "status": project.get("status",{}).get("name"),
        "code": project.get("code"),
        "estimatedcost":project.get("estimatedCost",{}).get("amount")
    }

def check_for_project_updates(dag_run):
    project_details = rail.result("get_project_details")
    for i in project_details:
        if i != "uri" and i in dag_run.conf and project_details[i] != dag_run.conf[i]:
            return True