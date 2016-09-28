###
###     IRDM GROUP PROJECT
###

library("sqldf", lib.loc="~/R/win-library/3.2")

#read all load data
load = read.csv("C:/Users/hp-pc/Documents/IRDM/Load_history.csv")

#filter for zone 1
load_z1 = sqldf("select * from load where zone_id = 1")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

##
## read temperature data for 11 stations
## 

##cache temperature data for 11 stations
master_temp = NULL

#read all temperature data
temper = read.csv("C:/Users/hp-pc/Documents/IRDM/temperature_history.csv")

#get temperature for staton 1
temper_z1 = sqldf("select * from temper where station_id = 1")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station1

load_temp = sqldf("select l.*,t.temper from
                   load_z1_hourly l 
                   left join temper_z1_hourly t on 
                   l.year = t.year and
                   l.month = t.month and
                   l.day = t.day and
                   l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper1'
master_temp$temper1 = load_temp$temper1

####################################################################
#### add temperature for station 2
#get temperature for staton 2
temper_z1 = sqldf("select * from temper where station_id = 2")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station2

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper2'
master_temp$temper2 = load_temp$temper2

####################################################################
#### add temperature for station 3
#get temperature for staton 3
temper_z1 = sqldf("select * from temper where station_id = 3")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station3

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper3'
master_temp$temper3 = load_temp$temper3

####################################################################
#### add temperature for station 4
#get temperature for staton 4
temper_z1 = sqldf("select * from temper where station_id = 4")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station4

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper4'
master_temp$temper4 = load_temp$temper4

####################################################################
#### add temperature for station 5
#get temperature for staton 5
temper_z1 = sqldf("select * from temper where station_id = 5")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station5

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper5'
master_temp$temper5 = load_temp$temper5

####################################################################
#### add temperature for station 6
#get temperature for staton 6
temper_z1 = sqldf("select * from temper where station_id = 6")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station6

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper6'
master_temp$temper6 = load_temp$temper6

####################################################################
#### add temperature for station 7
#get temperature for staton 7
temper_z1 = sqldf("select * from temper where station_id = 7")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station7

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper7'
master_temp$temper7 = load_temp$temper7

####################################################################
#### add temperature for station 8
#get temperature for staton 8
temper_z1 = sqldf("select * from temper where station_id = 8")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station8

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper8'
master_temp$temper8 = load_temp$temper8

####################################################################
#### add temperature for station 9
#get temperature for staton 9
temper_z1 = sqldf("select * from temper where station_id = 9")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station9

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper9'
master_temp$temper9 = load_temp$temper9

####################################################################
#### add temperature for station 10
#get temperature for staton 10
temper_z1 = sqldf("select * from temper where station_id = 10")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station10

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper10'
master_temp$temper10 = load_temp$temper10

####################################################################
#### add temperature for station 11
#get temperature for staton 11
temper_z1 = sqldf("select * from temper where station_id = 11")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

## join to get load for zone and temp for station11

load_temp = sqldf("select l.*,t.temper from
                  load_temp l 
                  left join temper_z1_hourly t on 
                  l.year = t.year and
                  l.month = t.month and
                  l.day = t.day and
                  l.hour = t.hour")

names(load_temp)[names(load_temp) == 'temper'] = 'temper11'
master_temp$temper11 = load_temp$temper11

all_temp = NULL
x=as.data.frame(master_temp[1])
all_temp = data.table(x)
names(all_temp) = c("temper1")
x=as.data.frame(master_temp[2])
all_temp$temper2 = data.table(x)
x=as.data.frame(master_temp[3])
all_temp$temper3 = data.table(x)
x=as.data.frame(master_temp[4])
all_temp$temper4 = data.table(x)
x=as.data.frame(master_temp[5])
all_temp$temper5 = data.table(x)
x=as.data.frame(master_temp[6])
all_temp$temper6 = data.table(x)
x=as.data.frame(master_temp[7])
all_temp$temper7 = data.table(x)
x=as.data.frame(master_temp[8])
all_temp$temper8 = data.table(x)
x=as.data.frame(master_temp[9])
all_temp$temper9 = data.table(x)
x=as.data.frame(master_temp[10])
all_temp$temper10 = data.table(x)
x=as.data.frame(master_temp[11])
all_temp$temper11= data.table(x)

                                 


#############################################################################################



#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 1 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 1

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#### stationarity

library(tseries)
load_temp$load_diff = c(1,diff(load_temp$load))
adf.test(load_temp$load_diff)
kpss.test(load_temp$load_diff)

load_temp$temper1_diff = c(1,diff(load_temp$temper1))
adf.test(load_temp$temper1_diff)
kpss.test(load_temp$temper1_diff)

#load_temp = load_temp[!is.na(load_temp$load)]


## select temperature series based on MI

#bin calculation as per sturges rule  ##15
log2(length(load_temp$load))

library(entropy)
res = NULL
y2d = discretize2d(load_temp$load,load_temp$temper1, numBins1=15, numBins2=15)
res[1] = mi.empirical(y2d)

y2d = discretize2d(load_temp$load,load_temp$temper2, numBins1=15, numBins2=15)
res[2] = mi.empirical(y2d)


library(vars)
var_data = cbind(req_cum_hour$load,req_cum_hour$temper1,req_cum_hour$temper2
                 ,req_cum_hour$temper3
                 ,req_cum_hour$temper4
                 ,req_cum_hour$temper5
                 ,req_cum_hour$temper6
                 ,req_cum_hour$temper7
                 ,req_cum_hour$temper8
                 ,req_cum_hour$temper9
                 ,req_cum_hour$temper10
                 ,req_cum_hour$temper11)


#VARselect(var_data,lag.max = 100,type="both")

model = VAR(var_data, p = 99, type = "both")

#plot(model,include=200)

pred = predict(model, n.ahead = nrow(val_z1_hourly), ci = 0.95)

#dev.off()

#ts.plot(pred$fcst$y1[,1])

#acf(residuals(model)[,1])

mape = data.table(pred$fcst$y1[,1],val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


##########################################################
####
###                       ZONE 2      
###
#############################################################



#filter for zone 1
load_z1 = sqldf("select * from load where zone_id = 2")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 2 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 1

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#### stationarity

library(tseries)
load_temp$load_diff = c(1,diff(load_temp$load))
adf.test(load_temp$load_diff)
kpss.test(load_temp$load_diff)

load_temp$temper1_diff = c(1,diff(load_temp$temper1))
adf.test(load_temp$temper1_diff)
kpss.test(load_temp$temper1_diff)

#load_temp = load_temp[!is.na(load_temp$load)]


## select temperature series based on MI

#bin calculation as per sturges rule  ##15
log2(length(load_temp$load))

library(entropy)
res = NULL
y2d = discretize2d(load_temp$load,load_temp$temper1, numBins1=15, numBins2=15)
res[1] = mi.empirical(y2d)

y2d = discretize2d(load_temp$load,load_temp$temper2, numBins1=15, numBins2=15)
res[2] = mi.empirical(y2d)


library(vars)
var_data = cbind(req_cum_hour$load,req_cum_hour$temper1,req_cum_hour$temper2
                 ,req_cum_hour$temper3
                 ,req_cum_hour$temper4
                 ,req_cum_hour$temper5
                 ,req_cum_hour$temper6
                 ,req_cum_hour$temper7
                 ,req_cum_hour$temper8
                 ,req_cum_hour$temper9
                 ,req_cum_hour$temper10
                 ,req_cum_hour$temper11)


#VARselect(var_data,lag.max = 100,type="both")

model = VAR(var_data, p = 99, type = "both")

#plot(model,include=200)

pred = predict(model, n.ahead = nrow(val_z1_hourly), ci = 0.95)

#dev.off()

#ts.plot(pred$fcst$y1[,1])

#acf(residuals(model)[,1])

mape = data.table(pred$fcst$y1[,1],val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)




##########################################################
####
###                       ZONE 3      
###
#############################################################



#filter for zone 1
load_z1 = sqldf("select * from load where zone_id = 3")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 3 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 1

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#### stationarity

library(tseries)
load_temp$load_diff = c(1,diff(load_temp$load))
adf.test(load_temp$load_diff)
kpss.test(load_temp$load_diff)

load_temp$temper1_diff = c(1,diff(load_temp$temper1))
adf.test(load_temp$temper1_diff)
kpss.test(load_temp$temper1_diff)

#load_temp = load_temp[!is.na(load_temp$load)]


## select temperature series based on MI

#bin calculation as per sturges rule  ##15
log2(length(load_temp$load))

library(entropy)
res = NULL
y2d = discretize2d(load_temp$load,load_temp$temper1, numBins1=15, numBins2=15)
res[1] = mi.empirical(y2d)

y2d = discretize2d(load_temp$load,load_temp$temper2, numBins1=15, numBins2=15)
res[2] = mi.empirical(y2d)


library(vars)
var_data = cbind(req_cum_hour$load,req_cum_hour$temper1,req_cum_hour$temper2
                 ,req_cum_hour$temper3
                 ,req_cum_hour$temper4
                 ,req_cum_hour$temper5
                 ,req_cum_hour$temper6
                 ,req_cum_hour$temper7
                 ,req_cum_hour$temper8
                 ,req_cum_hour$temper9
                 ,req_cum_hour$temper10
                 ,req_cum_hour$temper11)


#VARselect(var_data,lag.max = 100,type="both")

model = VAR(var_data, p = 99, type = "both")

#plot(model,include=200)

pred = predict(model, n.ahead = nrow(val_z1_hourly), ci = 0.95)

#dev.off()

#ts.plot(pred$fcst$y1[,1])

#acf(residuals(model)[,1])

mape = data.table(pred$fcst$y1[,1],val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)



##########################################################
####
###                       ZONE 4      
###
#############################################################



#filter for zone 1
load_z1 = sqldf("select * from load where zone_id = 4")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 4 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 1

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#### stationarity

library(tseries)
load_temp$load_diff = c(1,diff(load_temp$load))
adf.test(load_temp$load_diff)
kpss.test(load_temp$load_diff)

load_temp$temper1_diff = c(1,diff(load_temp$temper1))
adf.test(load_temp$temper1_diff)
kpss.test(load_temp$temper1_diff)

#load_temp = load_temp[!is.na(load_temp$load)]


## select temperature series based on MI

#bin calculation as per sturges rule  ##15
log2(length(load_temp$load))

library(entropy)
res = NULL
y2d = discretize2d(load_temp$load,load_temp$temper1, numBins1=15, numBins2=15)
res[1] = mi.empirical(y2d)

y2d = discretize2d(load_temp$load,load_temp$temper2, numBins1=15, numBins2=15)
res[2] = mi.empirical(y2d)


library(vars)
var_data = cbind(req_cum_hour$load,req_cum_hour$temper1,req_cum_hour$temper2
                 ,req_cum_hour$temper3
                 ,req_cum_hour$temper4
                 ,req_cum_hour$temper5
                 ,req_cum_hour$temper6
                 ,req_cum_hour$temper7
                 ,req_cum_hour$temper8
                 ,req_cum_hour$temper9
                 ,req_cum_hour$temper10
                 ,req_cum_hour$temper11)


#VARselect(var_data,lag.max = 100,type="both")

model = VAR(var_data, p = 99, type = "both")

#plot(model,include=200)

pred = predict(model, n.ahead = nrow(val_z1_hourly), ci = 0.95)

#dev.off()

#ts.plot(pred$fcst$y1[,1])

#acf(residuals(model)[,1])

mape = data.table(pred$fcst$y1[,1],val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


##########################################################
####
###                       ZONE 5      
###
#############################################################



#filter for zone 1
load_z1 = sqldf("select * from load where zone_id = 5")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 5 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 1

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#### stationarity

library(tseries)
load_temp$load_diff = c(1,diff(load_temp$load))
adf.test(load_temp$load_diff)
kpss.test(load_temp$load_diff)

load_temp$temper1_diff = c(1,diff(load_temp$temper1))
adf.test(load_temp$temper1_diff)
kpss.test(load_temp$temper1_diff)

#load_temp = load_temp[!is.na(load_temp$load)]


## select temperature series based on MI

#bin calculation as per sturges rule  ##15
log2(length(load_temp$load))

library(entropy)
res = NULL
y2d = discretize2d(load_temp$load,load_temp$temper1, numBins1=15, numBins2=15)
res[1] = mi.empirical(y2d)

y2d = discretize2d(load_temp$load,load_temp$temper2, numBins1=15, numBins2=15)
res[2] = mi.empirical(y2d)


library(vars)
var_data = cbind(req_cum_hour$load,req_cum_hour$temper1,req_cum_hour$temper2
                 ,req_cum_hour$temper3
                 ,req_cum_hour$temper4
                 ,req_cum_hour$temper5
                 ,req_cum_hour$temper6
                 ,req_cum_hour$temper7
                 ,req_cum_hour$temper8
                 ,req_cum_hour$temper9
                 ,req_cum_hour$temper10
                 ,req_cum_hour$temper11)


#VARselect(var_data,lag.max = 100,type="both")

model = VAR(var_data, p = 99, type = "both")

#plot(model,include=200)

pred = predict(model, n.ahead = nrow(val_z1_hourly), ci = 0.95)

#dev.off()

#ts.plot(pred$fcst$y1[,1])

#acf(residuals(model)[,1])

mape = data.table(pred$fcst$y1[,1],val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


c(5,5)

library(deepnet)

net = nn.train(as.matrix(load_temp$temper1),load_temp$load)

net
nn.predict(net,load_temp$temper1)


net = nn.train(as.matrix(load_temp$temper1), load_temp$load, initW = NULL, initB = NULL,
             hidden = c(5,5), activationfun = "sigm",
             
         learningrate = 0.8, momentum = 0.5, learningrate_scale = 3, output = "sigm",
         numepochs = 100, batchsize = 100, hidden_dropout = 0, visible_dropout = 0)

nn.predict(net,load_temp$temper1)


library(neuralnet)

net <- neuralnet(load ~ temper1,
                 load_temp, hidden = 4, lifesign = "full", 
                       linear.output = TRUE, act.fct = "tanh", threshold = 0.1)


test = c(33,24,56,45,34,54,34)

test = NULL
x=as.data.frame(load_temp$temper1)
test = data.table(x)
names(test) = c("temper1")
test$temper2 = load_temp$temper2
test$temper3 = load_temp$temper3
test$temper4 = load_temp$temper4
test$temper5 = load_temp$temper5
test$temper6 = load_temp$temper6
test$temper7 = load_temp$temper7

plot(net, rep="best")

res = compute(net,test)
res$net.result[1:100]



nn <- neuralnet(load~temper1+temper2+temper3+temper4,
                data=load_temp, hidden=2, err.fct="ce",
                linear.output=FALSE)

res = compute(nn,test)

library(caret)

test = c(1,2,3,4,5)

#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 1 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 1

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone


################################################################################
## get temperature for prediction across 11 station for missing week
#################################################################################

#get temperature for staton 1
temper_z1 = sqldf("select * from temper where station_id = 1")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp = NULL
x=as.data.frame(nn_temp1)
test_temp = data.table(x)
names(test_temp) = c("temper1")

#################################################################################

#get temperature for staton 2
temper_z1 = sqldf("select * from temper where station_id = 2")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper2 = as.data.frame(nn_temp1)

#################################################################################

#get temperature for staton 3
temper_z1 = sqldf("select * from temper where station_id = 3")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper3 = as.data.frame(nn_temp1)

#################################################################################

#get temperature for staton 4
temper_z1 = sqldf("select * from temper where station_id = 4")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper4 = as.data.frame(nn_temp1)

#################################################################################

#get temperature for staton 5
temper_z1 = sqldf("select * from temper where station_id = 5")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper5 = as.data.frame(nn_temp1)

#################################################################################

#get temperature for staton 6
temper_z1 = sqldf("select * from temper where station_id = 6")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper6 = as.data.frame(nn_temp1)

#################################################################################

#get temperature for staton 7
temper_z1 = sqldf("select * from temper where station_id = 7")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper7 = as.data.frame(nn_temp1)


#################################################################################

#get temperature for staton 8
temper_z1 = sqldf("select * from temper where station_id = 8")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper8 = as.data.frame(nn_temp1)


#################################################################################

#get temperature for staton 9
temper_z1 = sqldf("select * from temper where station_id = 9")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper9 = as.data.frame(nn_temp1)


#################################################################################

#get temperature for staton 10
temper_z1 = sqldf("select * from temper where station_id = 10")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper10 = as.data.frame(nn_temp1)

#################################################################################

#get temperature for staton 11
temper_z1 = sqldf("select * from temper where station_id = 11")

#define data frame for hourly load
temper_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(temper_z1))
{
  cursor = temper_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    temper_z1_hourly = rbind(temper_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
temper_z1_hourly = data.table(temper_z1_hourly)
col_headings <- c('station_id','year','month','day','hour','cum_hour','temper')
colnames(temper_z1_hourly) <- col_headings

nn_temp1 = sqldf("select temper from temper_z1_hourly where 
                 cum_hour > (select cum_hour from load_temp l
                 where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")


nn_temp1 = nn_temp1$temper[1:nrow(val_z1_hourly)]
test_temp$temper11 = as.data.frame(nn_temp1)


## temperatures for predicting have been cached in test_temp
##########################################################################################
#neural netwrok for zone 1 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4+temper5
               +temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

##########################################################
####
###                       ZONE 2      
###
#############################################################



#filter for zone 2
load_z1 = sqldf("select * from load where zone_id = 2")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 2 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 1

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4+temper5
               +temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

##########################################################
####
###                       ZONE 3      
###
#############################################################



#filter for zone 3
load_z1 = sqldf("select * from load where zone_id = 3")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 3 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4+temper5
               +temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

##########################################################
####
###                       ZONE 4      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 4")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 4 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4+temper5
               +temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

##########################################################
####
###                       ZONE 5      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 5")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 5 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

##########################################################
####
###                       ZONE 6      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 6")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 6 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

##########################################################
####
###                       ZONE 7      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 7")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 7 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


##########################################################
####
###                       ZONE 8      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 8")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 8 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


##########################################################
####
###                       ZONE 9      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 9")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 9 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


##########################################################
####
###                       ZONE 10      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 10")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 10 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


##########################################################
####
###                       ZONE 10      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 11")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 11 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)


##########################################################
####
###                       ZONE 12      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 12")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 12 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

##########################################################
####
###                       ZONE 13      
###
#############################################################



#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 13")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 13 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

test = NULL
x = as.data.frame(req_cum_hour$load[100:nrow(req_cum_hour)])
test = data.table(x)
names(test) = c("load")
test$temper1 = req_cum_hour$temper1[1:(nrow(req_cum_hour)-99)]
test$temper2 = req_cum_hour$temper2[1:(nrow(req_cum_hour)-99)]
test$temper3 = req_cum_hour$temper3[1:(nrow(req_cum_hour)-99)]
test$temper4 = req_cum_hour$temper4[1:(nrow(req_cum_hour)-99)]
test$temper5 = req_cum_hour$temper5[1:(nrow(req_cum_hour)-99)]
test$temper6 = req_cum_hour$temper6[1:(nrow(req_cum_hour)-99)]
test$temper7 = req_cum_hour$temper7[1:(nrow(req_cum_hour)-99)]
test$temper8 = req_cum_hour$temper8[1:(nrow(req_cum_hour)-99)]
test$temper9 = req_cum_hour$temper9[1:(nrow(req_cum_hour)-99)]
test$temper10 = req_cum_hour$temper10[1:(nrow(req_cum_hour)-99)]
test$temper11 = req_cum_hour$temper10[1:(nrow(req_cum_hour)-99)]

fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=test, repeats=25, size=3, decay=0.1,
               linout=TRUE)

rollmean(req_cum_hour$temper1,99,align="right")




fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)



################################################################################################3
###
###         HYBRID (NN with 99 day moving average)
###
################################################################################################
###########################################################################################

#######
###### for zone 1 hybrid

#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 1")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 1 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

test = NULL
x = as.data.frame(req_cum_hour$load[100:nrow(req_cum_hour)])
test = data.table(x)
names(test) = c("load")
test$temper1 = rollmean(req_cum_hour$temper1[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper2 = rollmean(req_cum_hour$temper2[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper3 = rollmean(req_cum_hour$temper3[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper4 = rollmean(req_cum_hour$temper4[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper5 = rollmean(req_cum_hour$temper5[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper6 = rollmean(req_cum_hour$temper6[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper7 = rollmean(req_cum_hour$temper7[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper8 = rollmean(req_cum_hour$temper8[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper9 = rollmean(req_cum_hour$temper9[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper10 = rollmean(req_cum_hour$temper10[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper11 = rollmean(req_cum_hour$temper11[1:(nrow(req_cum_hour)-1)],99,align = "right")





fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=test, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)



#####################################################################################################
###### for zone 1 hybrid

#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 2")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 2 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

test = NULL
x = as.data.frame(req_cum_hour$load[100:nrow(req_cum_hour)])
test = data.table(x)
names(test) = c("load")
test$temper1 = rollmean(req_cum_hour$temper1[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper2 = rollmean(req_cum_hour$temper2[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper3 = rollmean(req_cum_hour$temper3[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper4 = rollmean(req_cum_hour$temper4[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper5 = rollmean(req_cum_hour$temper5[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper6 = rollmean(req_cum_hour$temper6[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper7 = rollmean(req_cum_hour$temper7[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper8 = rollmean(req_cum_hour$temper8[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper9 = rollmean(req_cum_hour$temper9[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper10 = rollmean(req_cum_hour$temper10[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper11 = rollmean(req_cum_hour$temper11[1:(nrow(req_cum_hour)-1)],99,align = "right")





fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=test, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

#####################################################################################################
###### for zone 3 hybrid

#filter for zone 4
load_z1 = sqldf("select * from load where zone_id = 3")             ###change zone as per need


#load_z1 = load_z1[!is.na(load_z1$load)]



#define data frame for hourly load
load_z1_hourly = NULL

#cursor to arrange data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(load_z1))
{
  cursor = load_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+4])
    frame = c(cursor[,1],cursor[,2],cursor[,3],cursor[,4],k,cum_hour,temp)
    load_z1_hourly = rbind(load_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

library(data.table)

#load_z1_hourly is hourly load timeseries with cum_hour as hour
load_z1_hourly = data.table(load_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(load_z1_hourly) <- col_headings

load_z1_hourly = load_z1_hourly[!is.na(load_z1_hourly$load)]

## load cached temperature for 11 stations
load_temp = load_z1_hourly
load_temp$temper1 = all_temp$temper1
load_temp$temper2 = all_temp$temper2
load_temp$temper3 = all_temp$temper3
load_temp$temper4 = all_temp$temper4
load_temp$temper5 = all_temp$temper5
load_temp$temper6 = all_temp$temper6
load_temp$temper7 = all_temp$temper7
load_temp$temper8 = all_temp$temper8
load_temp$temper9 = all_temp$temper9
load_temp$temper10 = all_temp$temper10
load_temp$temper11 = all_temp$temper11


#############################################################################################
### get validation data for 1st missing week
#read all validation data
val = read.csv("C:/Users/hp-pc/Documents/IRDM/Benchmark.csv")

#filter for zone 1 and current week (march2015)
val_z1 = sqldf("select * from val where zone_id = 3 and year = 2005 and month = 3")       ############

#define data frame for hourly load
val_z1_hourly = NULL


#cursor to arrange validation data into a time series of hourly load
cum_hour = 1
for (i in 1:nrow(val_z1))
{
  cursor = val_z1[i,]
  for (k in 1:24)
  {
    temp = (cursor[,k+5])
    frame = c(cursor[,2],cursor[,3],cursor[,4],cursor[,5],k,cum_hour,temp)
    val_z1_hourly = rbind(val_z1_hourly,frame)
    cum_hour = cum_hour+1
  }
}

val_z1_hourly = data.table(val_z1_hourly)
col_headings <- c('zone_id','year','month','day','hour','cum_hour','load')
colnames(val_z1_hourly) <- col_headings

#get hourly data before 6/3/2005 from load_temp i.e. load data joined with temperature
#data from load_temp table before week to predict for zone 3

req_cum_hour = sqldf("select * from load_temp 
                     where cum_hour <= (select cum_hour from load_temp l
                     where (l.year = 2005 and l.month = 3 and l.day = 5 and l.hour = 24))")      #######change at next week not zone

#neural netwrok for zone 2 prediction

test = NULL
x = as.data.frame(req_cum_hour$load[100:nrow(req_cum_hour)])
test = data.table(x)
names(test) = c("load")
test$temper1 = rollmean(req_cum_hour$temper1[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper2 = rollmean(req_cum_hour$temper2[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper3 = rollmean(req_cum_hour$temper3[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper4 = rollmean(req_cum_hour$temper4[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper5 = rollmean(req_cum_hour$temper5[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper6 = rollmean(req_cum_hour$temper6[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper7 = rollmean(req_cum_hour$temper7[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper8 = rollmean(req_cum_hour$temper8[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper9 = rollmean(req_cum_hour$temper9[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper10 = rollmean(req_cum_hour$temper10[1:(nrow(req_cum_hour)-1)],99,align = "right")
test$temper11 = rollmean(req_cum_hour$temper11[1:(nrow(req_cum_hour)-1)],99,align = "right")





fit  <- avNNet(load ~ temper1+temper2+temper3+temper4
               +temper5+temper6+temper7+temper8+temper9+temper10+temper11,
               data=req_cum_hour, repeats=25, size=3, decay=0.1,
               linout=TRUE)

res = predict(fit, test_temp, type = "raw")

mape = data.table(res,val_z1_hourly$load)
col_headings <- c('fcast','act')
colnames(mape) <- col_headings

mape$err = (mape$act-mape$fcast)/mape$act
mape$err = abs(mape$err)

sum(mape$err)/nrow(mape)

