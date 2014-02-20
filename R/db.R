db_has_table <- function(con, table) {
  # SQL Server has no way to list temporary tables, so we always NA to
  # skip any local checks and rely on the database to throw informative errors
  NA
}

db_list_tables <- function(con) dbListTables(con)