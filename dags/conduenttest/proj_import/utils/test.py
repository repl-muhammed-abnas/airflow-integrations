# # import rail
# # 1.Create one log artifact - rail.CreateLogOperator
# # 2.write all the logs to the same artifact rail.WriteLogOperator
# # 3.foreach
# # 4.load the logs - rail.load_all_records(rail.ersult("create_log_artifiact"))
# # 5.write to a csv - rail.WriteCSVFileOperator
# # 6.genreate presigned url - rail.GeneratePresignedDownloadUrlOperator

# # 7.upload to SFTP


# # projectname;project code;action:validation,add,update,skipped;ecid;status

# # if_any_Records = rail.IFOperator(
# #             yes="create_log_artifact",
# #             no="send_no_records_email"
# #         )
 
# #         create_log_artifact = rail.CreateLogOperator(
# #             task_id="create_log_artifiact"
# #         )
 
# #         query_invalid
 
# #         log_invalid_records = rail.WriteLogOperator(
# #             task_id="log_invalid_records",
# #             log='{{result("create_log_artifiact")}}',
# #             severity="Exception",
# #             message="Invalid records",
# #             items='{{result("query_invalid")}}',
# #             properties=lambda item:{
# #                 "project_code": item["project_code"],
# #                 "project_name": item["project_name"],
# #                 "status": "Exception",
# #                 "action": "validation"
# #             }
# #         )
 
# #         query_valid =
 
        # tae = rail.ForEachOperator(
        #     task_id="abc",
        #     start_task="empty_Start",
        #     end_task="empty_end",
        #     items="{{result('query_valid')}}"
        # )
 
        # emppty_start = rail.EmptyOperator(task_id="")
 
        # debug = rail.PythonOperator(
        #     task_id="debug",
        #     python_callable=lambda: rail.result("abc")
        # )
 
 
        # emppty_start = rail.EmptyOperator(task_id="")