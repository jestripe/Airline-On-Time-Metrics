library(RODBC)
library(RPostgres)
library(sqldf)
library(forestmangr)
library(plyr)
library(ggplot2)
library(plotly)

# Set numerical output format
options(scipen = '99999')

# Connect to the database
ps_conn<- dbConnect(Postgres(), dbname = 'airlineontimemetrics',
                    host = '127.0.0.1', port = 5432,
                    user = USERNAME HERE, password = PASSWORD HERE)

# The SQL for retrieving the data from the database
delay_relationship_sql <- 'select "Year",
                                  "Month",
                                  "DayOfWeek",
                                  "FlightDate",
                                  "IATA_CODE_Reporting_Airline",
                                  "Flight_Number_Reporting_Airline",
                                  "Origin",
                                  "Dest",
                                  concat("Origin", "Dest") as CityPair,
                                  "DepDelay",
                                  "DepDelayMinutes",
                                  "DepDel15",
                                  "ArrDelay",
                                  "ArrDelayMinutes",
                                  "ArrDel15",
                                  "CRSElapsedTime",
                                  "ActualElapsedTime",
                                  "Flights"
                            from analysis.flight_data
                            where "Cancelled" = 0
                              and "Diverted" = 0;'

# Execute query and clean up some of the data
delay_relationship_df <- dbGetQuery(ps_conn, delay_relationship_sql)
delay_relationship_df$D0_dly <- ifelse(delay_relationship_df$DepDelayMinutes > 0 , 1, 0) # Defines logical/summary value for departure delays
delay_relationship_df$MergedAirline <- ifelse(delay_relationship_df$IATA_CODE_Reporting_Airline == 'CO', 'UA',
                                              ifelse(delay_relationship_df$IATA_CODE_Reporting_Airline == 'NW', 'DL',
                                                     ifelse(delay_relationship_df$IATA_CODE_Reporting_Airline == 'US', 'AA', delay_relationship_df$IATA_CODE_Reporting_Airline))) # Combines legacy airlines into their post-merger carriers

# Summarize the data by year and calculate D0 and A15 metrics, filter down to 2019 and earlier, create a label field for plots
delay_pct_df <- ddply(delay_relationship_df, .(Year), summarize,
                      flights = sum(Flights),
                      D0_dlys = sum(D0_dly),
                      A15_dlys = sum(ArrDel15))
delay_pct_df$D0 <- (1- (delay_pct_df$D0_dlys / delay_pct_df$flights))
delay_pct_df$A15 <- (1-(delay_pct_df$A15_dlys / delay_pct_df$flights))
delay_pct_df <- subset(delay_pct_df, Year < 2020)
delay_pct_df$DateLabel <- as.Date(paste0(delay_pct_df$Year, '-01-01'))

# Plot D0 versus A15
ggplot(delay_pct_df, aes(x = DateLabel)) +
  geom_line(aes(y = D0, linetype = 'D0'), color = 'black') +
  geom_line(aes(y = A15, linetype = 'A15'), color = 'dark grey')

# Filter data down to delays only and from 2019 prior.
delays_only_df <- subset(delay_relationship_df, DepDelayMinutes > 0 | ArrDelayMinutes > 0)
delays_only_df <- subset(delays_only_df, Year < 2020)

# If you don't understand why I use 42 as my seed Arthur says we can't be friends...
set.seed(42)

# Due to the sheer volume of data I need to sample the delay data in order to create a scatter plot of the departure/arrival delay relatioinship
sample_index <- sample.int(nrow(delays_only_df), size = nrow(delays_only_df) * 0.1)

# Creates sampled delay dataset for plotting
sample_data_df <- delays_only_df[sample_index,]

# Creates and displays the summary statistics of the linear regression model for departure and arrival delays
delay_minute_lm <- lm(ArrDelayMinutes ~ DepDelayMinutes, data = delays_only_df)
summary(delay_minute_lm)

# Plot the delay relationship as a scatter plot
ggplot(sample_data_df, aes(x = DepDelayMinutes, y = ArrDelayMinutes)) +
  geom_point() +
  xlab('Departure Delay Minutes') +
  ylab('Arrival Delay Minutes') +
  labs(caption = 'Note. Due to computational constraints this plot uses a random 10% sample of the data.')+
  theme(text = element_text(family = 'Times New Roman'),
          plot.caption = element_text(hjust = 0),
          plot.caption.position = 'plot')

# Creates a table of linear regression models for each year, sort it by year and create a label column for plotting
years_model <- lm_table(delays_only_df, model = ArrDelayMinutes ~ DepDelayMinutes,
                        .groups = 'Year') 
  years_model <- years_model[order(years_model$Year),]
  years_model$DateLabel <- as.Date(paste0(years_model$Year, '-01-01'))

# Create a linear model off of the r^2 values from the table of models above and find its slope
tmp_lm <- lm(Rsqr ~ DateLabel, years_model)
coef(tmp_lm)
slope <- tmp_lm[2]

# Plot the r^2 values from the model table with the line of best fit
ggplot(years_model, aes(y = Rsqr, x = DateLabel)) +
  geom_point() +
  geom_smooth(method = lm, se = FALSE)+
  ylim(.80, 1.0) +
  scale_x_date(date_labels = '%Y') +
  xlab('Year') +
  ylab('Delay Relationship R2') +
  theme(text = element_text(family = 'Times New Roman'))

# Calculate mean delay length by year
delay_stats_df <- ddply(delays_only_df, .(Year), summarize,
                        MeanDepDelay = mean(DepDelayMinutes),
                        MeanArrDelay = mean(ArrDelayMinutes))
  delay_stats_df$DelaySplit <- delay_stats_df$MeanArrDelay - delay_stats_df$MeanDepDelay
  delay_stats_df$DateLabel <- as.Date(paste0(delay_stats_df$Year, '-01-01'))

# Plot mean delay lengths by year
ggplot(delay_stats_df, aes(x = DateLabel)) +
  geom_line(aes(y = MeanDepDelay, linetype = 'Departure Delays'), color = 'black') +
  geom_line(aes(y = MeanArrDelay, linetype = "Arrival Delays"), color = 'dark grey') +
  ylim(0, 35) +
  scale_x_date(date_labels = '%Y') + 
  xlab('Year') +
  ylab('Minutes') +
  theme(text = element_text(family = 'Times New Roman'),
        legend.position = 'bottom')

# Find the number of days in the analysis period
unique_dates <- unique(delay_relationship_df$FlightDate)

# Identify the most heavily travled city pairs
city_pairs_df <- ddply(delay_relationship_df, .(MergedAirline, citypair), summarize,
                       Flights = sum(Flights))

# Remove airline so the largest overall city pairs are identified
city_pairs_no_airline_df <- ddply(city_pairs_df, .(citypair), summarize,
                                  Flights = sum(Flights),
                                  Airlines = length(unique(MergedAirline)))

# Find city piars with a volume equal to or reater than number of days in the analysis assumes at least once daily operation
most_freq_city_pairs_df <- subset(city_pairs_no_airline_df, Flights >= length(unique_dates))

# Samples roughly half of the city pairs identified above.
#########################################################################
### RUN THESE LINE ONLY ONCE OR THE DOWNSTREAM RESULTS WILL CHANGE!!! ###
city_pair_index <- sample.int(nrow(most_freq_city_pairs_df), size = 600)

# Idenifies the city piars to be sampled
most_freq_sample_df <- most_freq_city_pairs_df[city_pair_index,]

# Creates list of city pairs to filter the dataset on
city_pair_list <- as.list(most_freq_sample_df$citypair)
#########################################################################

# Pulls the data for 2009 and fliters down to only the needed columns
city_pair_2009 <- subset(delay_relationship_df, citypair %in% city_pair_list & Year == 2009)
cp_2009_df <- data.frame(Yr = city_pair_2009$Year, SBT = city_pair_2009$CRSElapsedTime)

# Set sample size
index_2009 <- sample.int(nrow(cp_2009_df), size = 25000)
# Sample data for 2009
cp_2009_df <- cp_2009_df[index_2009,]

# Pulls the data for 2019 and filters down to the required columns
city_pair_2019 <- subset(delay_relationship_df, citypair %in% city_pair_list & Year == 2019)
cp_2019_df <- data.frame(Yr = city_pair_2019$Year, SBT = city_pair_2019$CRSElapsedTime)

# Set sample size
index_2019 <- sample.int(nrow(cp_2019_df), size = 25000)
# Sample data for 2019
cp_2019_df <- cp_2019_df[index_2019, ]

# Combine 2009 and 2019 data for ease of analysis
SBT_data_df <- rbind(cp_2009_df, cp_2019_df)

# Find the mean delay times for 2009 and 2019
mean_sbt_2009 <- mean(city_pair_2009$CRSElapsedTime)
mean_sbt_2019 <- mean(city_pair_2019$CRSElapsedTime)

# See if the 2009 data fits a normal distribution, spolier it doesn't...
normality_test <- with(SBT_data_df, shapiro.test(SBT[Yr == 2009]))

# Normal T-test does not work in this situation...
SBT_distro <- t.test(SBT ~ Yr, data = SBT_data_df, var.equal = TRUE, alternative = 'less')

# QQplots to confirm non-normality
qqnorm(SBT_data_df$SBT, pch = 1, frame = FALSE)
qqline(SBT_data_df$SBT, lwd = 2)

# Box plot of mean SBTs for verification
ggplot(SBT_data_df, aes(x = SBT, fill = Yr)) +
  geom_histogram(binwidth = 15, alpha = 0.6, position = 'dodge') +
  scale_fill_manual(values = c('black', 'dark grey')) +
  ylab('Number of Flights') + 
  xlab('Scheduled Block Time') + 
  theme(text = element_text(family = 'Times New Roman'),
        legend.position = 'bottom') 
boxplot(SBT ~ Yr, data = SBT_data_df)

# Calcuate standard error for the data
jStdErr <- sqrt((var(cp_2009_df$SBT)/length(cp_2009_df$SBT)) + (var(cp_2019_df$SBT)/length(cp_2019_df$SBT)))

degFreedom <- length(SBT_data_df) - 2

# Calcualte T value and upper/lower limits
quartile <- qt(.95, degFreedom)
upperLim <- mean(mean_sbt_2009) + (quartile * (jStdErr))
lowerLim <- mean(mean_sbt_2009) - (quartile * (jStdErr))

# Find likely hood mean from 2019 falling in the distribution for 2009
distTest <- pnorm(mean_sbt_2019, mean_sbt_2009, jStdErr)
