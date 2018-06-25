library(bigrquery)
library(bigQueryR)
library(utf8)

qry <- paste0("SELECT * FROM ", "Mayak_Comagic",".","calls_report")
query_exec(query = qry ,
                      project = "model-creek-196411",
                      destination_table = "Mayak.comagic_calls_report",
                      create_disposition = "CREATE_IF_NEEDED",
                      write_disposition = "WRITE_APPEND",
           page_size = 0)

qry <- paste0("SELECT * FROM ", "Mayak_Comagic",".","communications_report")
query_exec(query = qry ,
                      project = "model-creek-196411",
                      destination_table = "Mayak.comagic_communications_report",
                      create_disposition = "CREATE_IF_NEEDED",
                      write_disposition = "WRITE_APPEND",
           page_size = 0)

qry <- paste0("SELECT * FROM ", "Mayak_Comagic",".","calls_report_finish_reason")
query_exec(query = qry ,
           project = "model-creek-196411",
           destination_table = "Mayak.comagic_calls_report_finish_reason",
           create_disposition = "CREATE_IF_NEEDED",
           write_disposition = "WRITE_APPEND",
           page_size = 0)