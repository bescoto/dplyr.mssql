require("dplyr.sqlserver")
require("RSQLServer")
require("stringr")
require("dplyr")
require("assertthat")

drv <- SQLServer()
con <- dbConnect(drv, sprintf("jdbc:sqlserver://%s; DatabaseName=%s", "qi03", "jydb"), "sig", "sig")
# info <- dbGetInfo(con)
# 
# sc <- src_sql("sqlserver", con, 
#         info = info, disco = function(con) dbDisconnect(con, "sqlserver"))
# 
# 
# require(dplyr.sqlserver)
db <- src_sqlserver("jydb", "qi03", "sig", "sig")
tbindex <- tbl(db, "LC_IndexComponentsWeight")
# tbindex <- tbl(db, "LC_IndexComponent")
class(tbindex)
system.time(foo <- as.data.frame(tbindex))
bar <- tbindex$query$fetch()
