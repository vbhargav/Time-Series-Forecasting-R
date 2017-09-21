#install.packages("RPresto")
#install.packages("data.table")

library(RPresto)
library(data.table)

#Setting up connection to presto
con <- dbConnect(
  RPresto::Presto(),
  host='http://presto.myteksi.net',
  port=8889,
  user='bhargav.vadhiraja@grabtaxi.com',
  schema='datamart',
  catalog='hive'
)

#Setting the time period for data
dt <- as.character(Sys.Date() - 1)
start_date <- as.character(Sys.Date() - 35)

#Query to be executed
qry <-  paste('SELECT date_local, cc.country_name, sum(rides) as rides FROM datamart.agg_bookings bk left join datamart.dim_cities_countries cc on (bk.city_id = cc.city_id) where bk.date_local <= ',dt,
              'and bk.date_local >=',start_date,
              'group by date_local, cc.country_name order by country_name, date_local',sep = "'")
result <- dbSendQuery(con,qry)

dat <- data.frame()

progress.bar <- NULL
while (!dbHasCompleted(result)) {
  chunk <- dbFetch(result)
  if (!NROW(iris)) {
    dat <- chunk
  } else if (NROW(chunk)) {
    dat <- rbind(dat, chunk)
  }
  stats <- dbGetInfo(result)[['stats']]
  if (is.null(progress.bar)) {
    progress.bar <- txtProgressBar(0, stats[['totalSplits']], style=3)
  } else {
    setTxtProgressBar(progress.bar, stats[['completedSplits']])
  }
}
close(progress.bar)

#Disconnect from Database
dbDisconnect(con)

#Check the data
head(dat)

#Modelling the trend
library(forecast)

#Subsetting only SG data
sg_rides <- dat[dat$country_name == 'Singapore', c(1,3)]

#Converting to time series
sg_rides_ts <- ts(sg_rides[,2],frequency = 7)

#Plotting the TS
plot(sg_rides_ts,xlab="Day",ylab="SG Rides Per Day")

#Forecasting
#Holt - Winters Seasonal
fit1 <- hw(sg_rides_ts,seasonal="additive")
fit2 <- hw(sg_rides_ts,seasonal="multiplicative")

fore_hw_add <- forecast(fit1,10)
fore_hw_mul <- forecast(fit2,10)

#Upload the forecast results to a presto table
