library("RGA")
library(bigrquery)

rga_auth <- authorize(client.id = "587513521921-j16pddusbijtm123akr64rl8ss9u1tst.apps.googleusercontent.com", client.secret = "cLicfgHl2YuolzDtlphb8o8A")
accs <- list_accounts(token = rga_auth)
prop <- list_webproperties(token = rga_auth)
views <- list_profiles(token = rga_auth)

start_day <- as.POSIXlt.date("2017-01-01")
end_day <- as.POSIXlt.date(substr(as.character(Sys.time()), 1, 10))
diff <- end_day - start_day

while (diff-1) {
  
  st_date<- substr(as.character(end_day - diff), 1, 10)
  diff <- diff - 1
  end_date <- substr(as.character(end_day - diff), 1, 10)
  diff <- diff - 1

  gaData <- get_ga(profileId = "ga:95248448",
                   start.date    = st_date,
                   end.date      = end_date,
                   dimensions     = "ga:date,ga:campaign,ga:sourceMedium,ga:adContent,ga:keyword",
                   metrics       = "ga:users,ga:newUsers,ga:sessions,ga:bounces,ga:entrances,ga:sessionDuration,ga:pageviews,ga:adClicks,ga:adCost,ga:impressions",
                   samplingLevel =  "HIGHER_PRECISION",
                   #                 filters = "ga:landingPagePath=@wt1-30234",
                   max.results   = 10000,
                   token = rga_auth)
  
  
  insert_upload_job(project = "model-creek-196411",
                    dataset = "Mayak",
                    table = "analytics",
                    values = gaData,
                    create_disposition = "CREATE_IF_NEEDED",
                    write_disposition = "WRITE_APPEND")
 
  rm(gaData)
  rm(st_date)
  rm(end_date)
}

