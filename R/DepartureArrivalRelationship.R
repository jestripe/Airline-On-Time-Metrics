library(RODBC)
library(RPostgres)
library(sqldf)
library(ggplot2)
library(plotly)

ps_conn<- dbConnect(Postgres(), dbname = 'airlineontimemetrics',
                    host = '127.0.0.1', port = 5432,
                    user = 'postgres', password = 'Em1lyAnn2')

delay_relationship_sql <- 'select "Year",
                                  "Month",
                                  "DayOfWeek",
                                  "FlightDate",
                                  "IATA_CODE_Reporting_Airline",
                                  "Flight_Number_Reporting_Airline",
                                  "DepDelay",
                                  "DepDelayMinutes",
                                  "DepDel15",
                                  "ArrDelay",
                                  "ArrDelayMinutes",
                                  "ArrDel15"
                            from analysis.flight_data
                            where "Cancelled" = 0
                              and "Diverted" = 0
                              and "Year" = 2017;'

delay_relationship_df <- dbGetQuery(ps_conn, delay_relationship_sql)

set.seed(42)

sample_index <- sample.int(nrow(delay_relationship_df), size = nrow(delay_relationship_df) * 0.1)

sample_data_df <- delay_relationship_df[sample_index,]

delay_minute_lm <- lm(ArrDelayMinutes ~ DepDelayMinutes, data = sample_data_df)
summary(delay_minute_lm)