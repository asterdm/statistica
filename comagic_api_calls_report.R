# получаем данные из api comagic
library("httr", lib.loc="~/R/R-3.4.4/library")
library("RGA")
library(bigrquery)

url <- "https://dataapi.comagic.ru/v2.0"

start_day <- as.POSIXlt.date("2018-06-04")
#yesterday <- as.POSIXlt.date(as.character(Sys.Date() - 1))
yesterday <- as.POSIXlt.date("2018-06-17")

day_diff <- yesterday - start_day 

while (day_diff) {
  
  day = yesterday - day_diff
  day_diff <- day_diff - 1
  req <- paste0('{
      "jsonrpc":"2.0",
                "id":"',day,'",
                "method":"get.calls_report",
                "params":{
                "access_token":"tai78ognipdgigkeo1w2lysobmzlx4lj34btklq2",
                "limit":10000,
                "date_from":"',day,' 00:00:00",
                "date_till":"',day,' 23:59:59",
                "fields":[
                "id",
                "start_time",
                "finish_time",
                "finish_reason",
                "direction",
                "source",
                "is_lost",
                "contact_phone_number",
                "communication_id",
                "wait_duration",
                "total_wait_duration",
                "talk_duration",
                "clean_talk_duration",
                "total_duration",
                "call_records",
                "ua_client_id",
                "is_transfer",
                "channel",
                "tags",
                "employees",
                "last_answered_employee_id",
                "last_answered_employee_full_name",
                "first_answered_employee_id",
                "first_answered_employee_full_name",
                "last_talked_employee_id",
                "last_talked_employee_full_name",
                "first_talked_employee_id",
                "first_talked_employee_full_name",
                "campaign_name",
                "campaign_id",
                "visit_other_campaign",
                "visitor_id",
                "person_id",
                "visitor_type",
                "visitor_session_id",
                "visits_count",
                "visitor_first_campaign_id",
                "visitor_first_campaign_name",
                "visitor_city",
                "visitor_region",
                "visitor_country",
                "visitor_device",
                "utm_source",
                "utm_medium",
                "utm_term",
                "utm_content",
                "utm_campaign"

                ]
                }
  }')
  
    res <- POST(url,body = req, encode = "json", verbose())
    out <- jsonlite::fromJSON(content(res, "text"),flatten = TRUE)
    out_table <- data.frame(out$result$data)
    out_table$employees <- paste(out_table$employees,sep = ",")
    out_table$tags <- paste(out_table$tags,sep = ",")
    out_table$call_records <- paste(out_table$call_records,sep = ",")
    
    
    out_table$utm_content <- as.character(out_table$utm_content)
    out_table$utm_campaign <- as.character(out_table$utm_campaign)
    out_table$utm_term <- as.character(out_table$utm_term)
    out_table$utm_source <- as.character(out_table$utm_source)
    out_table$utm_medium <- as.character(out_table$utm_medium)
    
    out_table$last_talked_employee_full_name <- as.character(out_table$last_talked_employee_full_name)
    out_table$last_talked_employee_id <- as.integer(out_table$last_talked_employee_id)
    out_table$last_answered_employee_id <- as.integer(out_table$last_answered_employee_id)
    out_table$last_answered_employee_full_name <- as.character(out_table$last_answered_employee_full_name)
    
    out_table$first_talked_employee_id <- as.integer(out_table$first_talked_employee_id)
    out_table$first_answered_employee_id<- as.integer(out_table$first_answered_employee_id)
    out_table$first_talked_employee_full_name <- as.character(out_table$first_talked_employee_full_name)
    out_table$first_answered_employee_full_name <- as.character(out_table$first_answered_employee_full_name)
    #out_table$communication_type <- paste(out_table$communication_type,sep = ",")
    
    out_table$visitor_session_id <- as.integer(out_table$visitor_session_id)
    
    insert_upload_job(project = "model-creek-196411",
                      dataset = "Mayak",
                      table = "comagic_calls_report_r",
                      values = out_table,
                      create_disposition = "CREATE_IF_NEEDED",
                      write_disposition = "WRITE_APPEND")
  
    rm(out)
    rm(out_table)
    rm(res)
  
  
}


    
