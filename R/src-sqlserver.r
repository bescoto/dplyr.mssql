#' create "src_sqlserver" object
#' @import dplyr
#' @import RMSSQL
#' @import DBI
#' @import assertthat
#' @export
src_mssql <- function(dbname=NULL, host=NULL, user='', ...) {
  drv <- MSSQLServer()
  con <- dbConnect(drv, dbname=dbname, host=host, user=user, ...)
  info <- list(host=host, dbname=dbname, user=user)
  return(src_sql("mssql", con, info=info))
}

#' @export
tbl.src_mssql <- function(src, from, ...) {
  tbl_sql("mssql", src = src, from = from, ...)
}


#' @export
src_desc.src_mssql <- function(x) {
  return(with(x$info, sprintf("MS SQL Server [%s@%s/%s]", user, host, dbname)))
}


#' @export
translate_env.src_sqlserver <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
                   n = function() sql("count(*)"),
                   sd =  sql_prefix("stddev_samp"),
                   var = sql_prefix("var_samp"),
                   paste = function(x, collapse) build_sql("group_concat(", x, collapse, ")")
    )
  )
}

#' @export
sql_select.MSSQLServerConnection <- function(
    con, select, from, where=NULL, group_by=NULL,
    having=NULL, order_by=NULL, limit=NULL, offset=NULL) {
  out <- vector("list", 7)
  names(out) <- c("select", "from", "where", "group_by", "having", "order_by",
                  "offset")

  assert_that(is.character(select), length(select) > 0L)

  if (!is.null(limit) || !is.null(order_by)) {
    if (is.null(limit)) {
      # We can't do an order_by inside a subquery without adding an
      # arbitrary limit.  The limit below assumes MS SQL uses 8 byte
      # integers (SQL bigint).
      out$select <- build_sql("SELECT TOP 9223372036854775807 ",
                              escape(select, collapse = ", ", con = con),
                              con = con)
    } else {
      assert_that(is.integer(limit), length(limit) == 1L)
      out$select <- build_sql("SELECT TOP ", limit, " ",
                              escape(select, collapse = ", ", con = con),
                              con = con)
    }
  } else {
    out$select <- build_sql("SELECT ", escape(select, collapse = ", ", con = con))
  }

  assert_that(is.character(from), length(from) == 1L)
  out$from <- build_sql("FROM ", from, con = con)

  if (length(where) > 0L) {
    assert_that(is.character(where))
    out$where <- build_sql("WHERE ",
                           escape(where, collapse = " AND ", con = con))
  }

  if (!is.null(group_by)) {
    assert_that(is.character(group_by), length(group_by) > 0L)
    out$group_by <- build_sql("GROUP BY ",
                              escape(group_by, collapse = ", ", con = con))
  }

  if (!is.null(having)) {
    assert_that(is.character(having), length(having) == 1L)
    out$having <- build_sql("HAVING ",
                            escape(having, collapse = ", ", con = con))
  }

  if (!is.null(order_by)) {
    assert_that(is.character(order_by), length(order_by) > 0L)
    # we can't have any repetitions in the order_by columns.  This
    # could happen if a column got added automatically because of a
    # group_by.
    order_by <- unique(order_by)
    out$order_by <- build_sql("ORDER BY ",
                              escape(order_by, collapse = ", ", con = con))
  }

  if (!is.null(offset)) {
    assert_that(is.integer(offset), length(offset) == 1L)
    out$offset <- build_sql("OFFSET ", offset, con = con)
  }

  escape(unname(compact(out)), collapse = "\n", parens = FALSE, con = con)
}

#' @export
db_query_fields.MSSQLServerConnection <- function(conn, sql, ...) {
  # Override the default query
  #
  # The default wants to run dbListFields(...) on the query, which
  # doesn't make sense to me
  fields <- build_sql("SELECT * FROM ", sql, " WHERE 0=1", con=conn)
  qry <- dbSendQuery(conn, fields)
  on.exit(dbClearResult(qry))
  return(dbColumnInfo(qry)$field.name)
}

#' @export
src_translate_env.src_mssql <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
                   n = function() sql("count(*)"),
                   sd =  sql_prefix("stdev"),
                   var = sql_prefix("var")
    )
  )
}

