import json
from datetime import datetime
from uuid import uuid4
import rail

null = None

def create_project(dag_run):
    
    start_date = rail.parse_date(datetime.strptime
        (dag_run.conf["start_date"], "%m/%d/%Y").strftime("%Y-%m-%d"),"%Y-%m-%d") if dag_run.conf["start_date"]  else null
    
    end_date = rail.parse_date(datetime.strptime
        (dag_run.conf["end_date"], "%m/%d/%Y").strftime("%Y-%m-%d"),"%Y-%m-%d") if dag_run.conf["end_date"]  else null


    return {
  "target": null,
  "modifications": {
    "nameToApply": {
      "value": dag_run.conf["project_name"]
    },
    "codeToApply": {
      "value": dag_run.conf["project_code"]
    },
    "descriptionToApply": null,
    "percentCompletedToApply": null,
    "startDateToApply": {
      "date": {**start_date}
    },
    "endDateToApply": {
      "date": {**end_date}
    },
    "billingTypeToApply": null,
    "clientBillingAllocationMethodToApply": "urn:replicon:client-billing-allocation-method:split",
    "clientAssignmentsSchedulesToApply": {
      "clients": [
        {
          "client": {
            "uri": "urn:replicon-tenant:b0a5ef71e56344c6b3901e15b9ec9487:client:3",
            "name": null,
            "code": null,
            "parameterCorrelationId": null
          },
          "costAllocationPercentage": "100"
        }
      ],
      "effectiveDate": null
    },
    "statusToApply": {
      "uri": null,
      "name": dag_run.conf["project_status"]
    },
    "projectWorkflowStateToApply": null,
    "clientRepresentativeToApply": null,
    "programToApply": null,
    "projectLeaderToApply": {
      "user": {
        "uri": "urn:replicon-tenant:b0a5ef71e56344c6b3901e15b9ec9487:user:2",
        "loginName": null,
        "employeeId": null,
        "parameterCorrelationId": null
      }
    },
    "isProjectLeaderApprovalRequired": null,
    "costTypeToApply": null,
    "isTimeEntryAllowed": null,
    "expenseCodesToApply": null,
    "estimatedHoursToApply": null,
    "budgetedHoursToApply": null,
    "estimatedCostToApply": {
      "value": {
        "amount": dag_run.conf["est_cost"],
        "currency": {
          "uri": "urn:replicon-tenant:b0a5ef71e56344c6b3901e15b9ec9487:currency:1",
          "name": null,
          "symbol": null
        }
      }
    },
    "budgetedCostToApply": null,
    "expenseBudgetedCostToApply": null,
    "totalEstimatedContractValueToApply": null,
    "defaultBillingCurrencyToApply": null,
    "timeAndMaterials": {
      "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
      "billingRateFrequency": null,
      "billingRateFrequencyDuration": null,
      "billingRates": []
    },
    "billingContractToApply": null,
    "fixedBid": null,
    "customFieldsToApply": [],
    "resourceAssignmentModifications": null,
    "resourceProjectAssignmentModifications": null,
    "billingContractModifications": null,
    "keyValuesToApply": [],
    "objectExtensionFieldsToApply": [],
    "portfolioToApply": null,
    "locationToApply": null,
    "divisionToApply": null,
    "serviceCenterToApply": null,
    "costCenterToApply": null,
    "departmentGroupToApply": null,
    "employeeTypeGroupToApply": null
  },
  "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
  "unitOfWorkId": str(uuid4()),
}

def get_custom_field_update_request(dag_run):
    project_details = rail.result("get_project_details")
    project_exists = project_details and project_details.get("uri")
 
    start_date = rail.parse_date(datetime.strptime
        (dag_run.conf["start_date"], "%m/%d/%Y").strftime("%Y-%m-%d"),"%Y-%m-%d") if dag_run.conf["start_date"]  else null
   
    end_date = rail.parse_date(datetime.strptime
        (dag_run.conf["end_date"], "%m/%d/%Y").strftime("%Y-%m-%d"),"%Y-%m-%d") if dag_run.conf["end_date"]  else null
 
    if project_exists:
        modifications = {}
 
       
        if dag_run.conf.get("project_name") and project_details.get("name") != dag_run.conf["project_name"]:
            modifications["nameToApply"] = {"value": dag_run.conf["project_name"]}
 
        if dag_run.conf.get("project_code") and project_details.get("code") != dag_run.conf["project_code"]:
            modifications["codeToApply"] = {"value": dag_run.conf["project_code"]}
 
        if dag_run.conf.get("project_status") and project_details.get("status") != dag_run.conf["project_status"]:
            modifications["statusToApply"] = {"uri": None, "name": dag_run.conf["project_status"]}
 
       
        existing_start = project_details.get("start_date")
        if start_date and existing_start != start_date:
            modifications["startDateToApply"] = {"date": start_date}
 
        existing_end = project_details.get("end_date")
        if end_date and existing_end != end_date:
            modifications["endDateToApply"] = {"date": end_date}
 
       
        current_cost = float(project_details.get("estimatedcost", 0))
        if dag_run.conf.get("est_cost") and current_cost != float(dag_run.conf["est_cost"]):
            modifications["estimatedCostToApply"] = {
                "value": {
                    "amount": float(dag_run.conf["est_cost"]),
                    "currency": {
                        "uri": "urn:replicon-tenant:b0a5ef71e56344c6b3901e15b9ec9487:currency:1",
                        "name": None,
                        "symbol": None
                    }
                }
            }
 
        modifications.update({
            "descriptionToApply": None,
            "percentCompletedToApply": None,
            "billingTypeToApply": None,
            "clientBillingAllocationMethodToApply": None,
            "clientAssignmentsSchedulesToApply": None,
            "projectWorkflowStateToApply": None,
            "clientRepresentativeToApply": None,
            "programToApply": None,
            "projectLeaderToApply": None,
            "isProjectLeaderApprovalRequired": None,
            "costTypeToApply": None,
            "isTimeEntryAllowed": None,
            "expenseCodesToApply": None,
            "estimatedHoursToApply": None,
            "budgetedHoursToApply": None,
            "budgetedCostToApply": None,
            "expenseBudgetedCostToApply": None,
            "totalEstimatedContractValueToApply": None,
            "defaultBillingCurrencyToApply": None,
            "timeAndMaterials": None,
            "billingContractToApply": None,
            "fixedBid": None,
            "customFieldsToApply": [],
            "resourceAssignmentModifications": None,
            "resourceProjectAssignmentModifications": None,
            "billingContractModifications": None,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [],
            "portfolioToApply": None,
            "locationToApply": None,
            "divisionToApply": None,
            "serviceCenterToApply": None,
            "costCenterToApply": None,
            "departmentGroupToApply": None,
            "employeeTypeGroupToApply": None
        })
 
        return {
            "target": {"uri": project_details["uri"]},
            "modifications": modifications,
            "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
            "unitOfWorkId": str(uuid4())
        }