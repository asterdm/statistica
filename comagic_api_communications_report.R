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
                "method":"get.communications_report",
                "params":{
                "access_token":"tai78ognipdgigkeo1w2lysobmzlx4lj34btklq2",
                "limit":10000,
                "date_from":"',day,' 00:00:00",
                "date_till":"',day,' 23:59:59",
                "fields":[
                "id",
                "communication_type",
                "date_time",
                "campaign_name",
                "campaign_id",
                "utm_source",
                "utm_medium",
                "utm_term",
                "utm_content",
                "attributes",
                "utm_campaign",
                "communication_type",
                "communication_number",
                "channel",
                "referrer",
                "visitor_id",
                "visitor_region",
                "visitor_device",
                "visitor_city",
                "visits_count",
                "visitor_type",
                "tags"
                ]
                }
  }')
  
    res <- POST(url,body = req, encode = "json", verbose())
    out <- jsonlite::fromJSON(content(res, "text"),flatten = TRUE)
    out_table <- data.frame(out$result$data)
    out_table$attributes <- paste(out_table$attributes,sep = ",")
    out_table$tags <- paste(out_table$tags,sep = ",")
    out_table$utm_content <- as.character(out_table$utm_content)
    #out_table$communication_type <- paste(out_table$communication_type,sep = ",")
    
    insert_upload_job(project = "model-creek-196411",
                      dataset = "Mayak",
                      table = "comagic_communications_report_r",
                      values = out_table,
                      create_disposition = "CREATE_IF_NEEDED",
                      write_disposition = "WRITE_APPEND")
  
    rm(out)
    rm(out_table)
    rm(res)
  
  
}


    
