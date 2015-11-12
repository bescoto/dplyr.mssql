#' @import RJDBC
sql_insert_into <- function(con, table, values) {

  MAX_INSERT_ROWS <- 1000
  qry_run <- function(con, sql,
                      show = getOption("dplyr.show_sql", default=FALSE),
                      explain = getOption("dplyr.explain_sql", default=FALSE)) {
    if (show) message(sql)
    if (explain) message(qry_explain(con, sql))

    dbSendUpdate(con, sql)

    invisible(NULL)
  }

  cols <- lapply(values, escape, collapse = NULL, parens = FALSE, con = con)
  col_mat <- matrix(unlist(cols, use.names = FALSE), nrow = nrow(values))

  rows <- apply(col_mat, 1, paste0, collapse = ", ")
  len <- floor(length(rows) / MAX_INSERT_ROWS)
  remainder <- length(rows) %% MAX_INSERT_ROWS
  expand_rows <- function(len) {
    if(len>0)
      unlist(lapply(1:len, function(x) rep(x, MAX_INSERT_ROWS)))
    else
      c()
  }
  group <- c(expand_rows(len), rep(len+1, remainder))

  grouped_rows <- split(rows, group)

  for(subrows in grouped_rows) {
    values <- paste0("(", subrows, ")", collapse = "\n, ")
    sql <- build_sql("INSERT INTO ", ident(table), " VALUES ", sql(values))
    qry_run(con, sql)
  }

}
