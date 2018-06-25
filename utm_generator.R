# определим функцию конкентации и загрузим файл с исходными данными

"%.%" <- function(...){
      paste0(...)
}


file_data <- read.csv(file.choose())


#1 шагэто различные проверки входящих данных пока его пропустим
#2.1 удалим якоря

#2.2 добавим к юрл знаки перечисления параметров

utm_url <- file_data$url[(file_data$url != "") & (!is.na(file_data$url))] 
utm_url_1 <- ifelse(grepl("?",utm_url,fixed = TRUE),"&", "?")
utm_url <- utm_url %.% utm_url_1

#utm_url_2 <- utm_url[!grepl("?",utm_url,fixed = TRUE)] %.% "?"
#utm_url <- c(utm_url_1, utm_url_2)


#3 добавим параметры для каждого столбца

utm_campaign <- "utm_campaign=" %.% file_data$utm_campaign[file_data$utm_campaign != "" & !is.na(file_data$utm_campaign)]
utm_source <- "&utm_source=" %.% file_data$utm_source[file_data$utm_source != "" & !is.na(file_data$utm_source)]
utm_medium <- "&utm_medium=" %.% file_data$utm_medium[file_data$utm_medium != "" & !is.na(file_data$utm_medium)]
utm_content <- "&utm_content=" %.% file_data$utm_content[file_data$utm_content != "" & !is.na(file_data$utm_content)]
utm_term <- "&utm_term=" %.% file_data$utm_term[file_data$utm_term != "" & !is.na(file_data$utm_term)]

#4 комбинирование всех параметров и запись в файл

res <- expand.grid(utm_url,utm_term,utm_medium,utm_source,utm_content,utm_campaign)
utm_output = sprintf("%s%s%s%s%s%s",res[,1], res[,6], res[,3],res[,4],res[,5],res[,2])
setwd("C:/utm")
write.csv(utm_output,"utm_generated.csv")
