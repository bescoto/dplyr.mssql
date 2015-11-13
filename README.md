
dplyr.mssql
===============

Microsoft SQL Server source driver for dplyr using the RMSSQL package
(Microsoft JDBC based).


requirements
===============

    devtools::install_github("bescoto/RMSSQL")
    devtools::install_github("bescoto/dplyr.mssql") # this package

Also you'll need these packages: DBI, dplyr, RJDBC, assertthat, dplyr, stringr


Code example
===============

```
library(dplyr)
library(RMSSQL)
library(dplyr.mssql)

my_db <- src_mssql(host='myDBServer',
                   dbname='myDatabase',
                   user='sa', password='mypw')

## WARNING: These three lines may take a while (20 min?)
# If the "flights" table is already in the database, you can skip
# these next three lines, which currently are commented out.

#library(nycflights13)
#try(dbRemoveTable(my_db$con, "flights"))
#flights.mssql <- copy_to(my_db, flights, temporary=FALSE)

## Once "flights" is in the database, can use the line instead
flights.mssql <- tbl(my_db, sql("select * from flights"))

c1 <- filter(flights.mssql, year == 2013, month == 1, day == 1)
c2 <- select(c1, year, month, day, carrier, dep_delay, air_time, distance)
c3 <- mutate(c2, speed = distance / air_time * 60)
c4 <- arrange(c3, year, month, day, carrier)
c4$query
c4
c5 <- group_by(c4, year) %>% summarize(avg.speed=mean(speed))
c5$query
c5

by_tailnum <- group_by(flights.mssql, tailnum)
delay <- summarise(by_tailnum,
  test.var = var(distance),
  test.sd = sd(distance),
  test.min = min(distance),
  test.max = max(distance),
  count = n(),
  dist = mean(distance),
  delay = mean(arr_delay)
)
delay <- filter(delay, count > 20, dist < 2000)
delay_local <- collect(delay)
delay$query
delay

########### Quick example with Windows authentication (instead of pw)
#
# NOTE: you will need to have the appropriate 32/64 bit
# sqljdbc_auth.dll library accessible in your PATH.  This will only
# work on windows.

my_db <- src_mssql(host='myDBServer',
                   dbname='myDatabase',
                   win.auth=TRUE)

flights.mssql <- tbl(my_db, sql("select * from flights"))
select(flights.mssql, year:day, dep_delay, arr_delay)
x <- filter(flights.mssql, dep_delay > 240)
x

```