library(devtools)
library(ryandexdirect)
library("RGA")
library(bigrquery)

aut <- yadirAuth()


start_day <- as.POSIXlt.date("2017-12-01")
yesterday <- as.POSIXlt.date(as.character(Sys.Date() - 1))

day_diff <- yesterday - start_day 

while (day_diff) {
  
  day = yesterday - day_diff
  day_diff <- day_diff - 1

campaign_stat <- yadirGetReport( ReportType = "CAMPAIGN_PERFORMANCE_REPORT",
                                 DateRangeType = "CUSTOM_DATE",
                                 DateFrom = day,
                                 DateTo = day,
                                 FieldNames = c("Date", "CampaignName","Impressions","Clicks","Cost","AdFormat","AdNetworkType","CampaignType","Device","Gender","Slot","CarrierType", "CriteriaType"),
                                 FilterList = NULL,
                                 IncludeVAT = "YES",
                                 IncludeDiscount = "NO",
                                 Login = "stylishkitchen@yandex.ru"
                                 )

SEARCH_QUERY_field_name <- c( "AvgClickPosition",
                              "AvgImpressionPosition",
                              "CriteriaType",
                              "MatchType",
                              "Placement",
                              "CampaignId",
                              "CampaignName",
                              "CampaignType",
                              "Clicks",
                              "Cost",
                              "Date",
                              "Impressions",
                              "Query"
                            )

search_stat <- yadirGetReport(ReportType = "SEARCH_QUERY_PERFORMANCE_REPORT",
                              DateRangeType = "CUSTOM_DATE",
                              DateFrom = day,
                              DateTo = day,
                              FieldNames = SEARCH_QUERY_field_name,
                              FilterList = NULL,
                              IncludeVAT = "YES",
                              IncludeDiscount = "NO",
                              Login = "stylishkitchen@yandex.ru"
                            )

insert_upload_job(project = "model-creek-196411",
                  dataset = "Stilnie_Kuhni",
                  table = "yandex_direct_campign_list",
                  values = campaign_stat,
                  create_disposition = "CREATE_IF_NEEDED",
                  write_disposition = "WRITE_APPEND")

insert_upload_job(project = "model-creek-196411",
                  dataset = "Stilnie_Kuhni",
                  table = "yandex_direct_query_list",
                  values = search_stat,
                  create_disposition = "CREATE_IF_NEEDED",
                  write_disposition = "WRITE_APPEND")

rm(campaign_stat)
rm(search_stat)
rm(day)
}
