library("RGA")
library(bigrquery)

rga_auth <- authorize(client.id = "763027251379-oi1e09vt1ihldj5hopjeud1hfqccqnpt.apps.googleusercontent.com", client.secret = "50OtdUyjIyMQ1Y4Lhd3FWrtO")
accs <- list_accounts(token = rga_auth)
prop <- list_webproperties(token = rga_auth)
views <- list_profiles(token = rga_auth)
gaData <- get_ga(profileId = "ga:80806929",
                 start.date    = "2018-04-22",
                 end.date      = "2018-04-30",
                 dimensions     = "ga:date,ga:campaign,ga:sourceMedium,ga:adContent,ga:keyword",
                 metrics       = "ga:users,ga:newUsers,ga:sessions,ga:goal16Starts,ga:goal17Starts,ga:bounces,ga:entrances,ga:sessionDuration,ga:pageviews",
                 samplingLevel =  "HIGHER_PRECISION",
                 filters = "ga:landingPagePath=@wt1-30234",
                 max.results   = 10000,
                 token = rga_auth)


insert_upload_job(project = "model-creek-196411",
                  dataset = "Miele_WT1",
                  table = "analytics",
                  values = gaData,
                  create_disposition = "CREATE_IF_NEEDED",
                  write_disposition = "WRITE_APPEND")
