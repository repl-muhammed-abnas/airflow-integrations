import rail
from capgemini_test.time_export.utils import request_payload



def time_data_export(group_id):

    with rail.TaskGroup(group_id=group_id) as tg:

        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.create_time_export_payload
        )

        execute_export, wait_for_export = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_export.task_id
        )

        get_export_uri = rail.RepliconServiceOperator(
            task_id='get_export_uri',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + create_export.task_id + "') }}" # this is the payload passing
            },
            data_handler=request_payload.retrieve_export_uri # working with response for filtering task
        )

        create_export_status_complete_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_complete_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_complete_batch_payload(get_export_uri.task_id)
        )

        execute_export_status_complete_batch, wait_for_export_status_complete_batch = rail.batch_execution(
            group_id='execute_time_export_status_complete_batch',
            creation_task_id=create_export_status_complete_batch.task_id
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda: request_payload.get_create_download_batch(get_export_uri.task_id)
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + create_download_batch.task_id + "') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('" + get_download_url.task_id + "') }}",
        )
        
        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('" + download_export.task_id + "') }}",
            delimiter=';'
        )
        
        create_export >> execute_export >> wait_for_export >> get_export_uri >> create_export_status_complete_batch >> execute_export_status_complete_batch >> wait_for_export_status_complete_batch >> create_download_batch
        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url
        get_download_url >> download_export >> load_export
        
        return tg