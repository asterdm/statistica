library(devtools)
library(ryandexdirect)
library("RGA")
library(bigrquery)

aut <- yadirAuth("mov-mayak-ss@yandex.ru")


start_day <- as.POSIXlt.date("2018-03-12")
yesterday <- as.POSIXlt.date(as.character(Sys.Date() - 1))

day_diff <- yesterday - start_day 

CAMPAIGN_field_name <- c( "Date",
                          "CampaignId",
                          "CampaignName",
                          "Impressions",
                          "Clicks",
                          "Cost",
                          "AdFormat",
                          "AdNetworkType",
                          "CampaignType",
                          "Device",
                          "Gender",
                          "Slot",
                          "CarrierType",
                          "CriteriaType",
                          "LocationOfPresenceId",
                          "LocationOfPresenceName",
                          "MatchType"
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

while (day_diff) {
  
  day = yesterday - day_diff
  day_diff <- day_diff - 1
  

  
  

campaign_stat <- yadirGetReport( ReportType = "CAMPAIGN_PERFORMANCE_REPORT",
                                 DateRangeType = "CUSTOM_DATE",
                                 DateFrom = day,
                                 DateTo = day,
                                 FieldNames = CAMPAIGN_field_name,
                                 FilterList = NULL,
                                 IncludeVAT = "YES",
                                 IncludeDiscount = "NO",
                                 Login = "mov-mayak-ss@yandex.ru"
                                 )



search_stat <- yadirGetReport(ReportType = "SEARCH_QUERY_PERFORMANCE_REPORT",
                              DateRangeType = "CUSTOM_DATE",
                              DateFrom = day,
                              DateTo = day,
                              FieldNames = SEARCH_QUERY_field_name,
                              FilterList = NULL,
                              IncludeVAT = "YES",
                              IncludeDiscount = "NO",
                              Login = "mov-mayak-ss@yandex.ru"
                            )

insert_upload_job(project = "model-creek-196411",
                  dataset = "Mayak",
                  table = "yandex_direct_campign_list",
                  values = campaign_stat,
                  create_disposition = "CREATE_IF_NEEDED",
                  write_disposition = "WRITE_APPEND")

insert_upload_job(project = "model-creek-196411",
                  dataset = "Mayak",
                  table = "yandex_direct_query_list",
                  values = search_stat,
                  create_disposition = "CREATE_IF_NEEDED",
                  write_disposition = "WRITE_APPEND")

rm(campaign_stat)
rm(search_stat)
rm(day)
}
