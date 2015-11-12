#' @import stringr
#' @import RJDBC
#' @import assertthat
#' @export
copy_to.src_mssql <- function(dest, df, name = deparse(substitute(df)),
                                  types = NULL, temporary = TRUE, indexes = NULL,
                                  analyze = TRUE, create = TRUE, ...) {
  assert_that(is.data.frame(df), is.string(name), is.flag(temporary))

  qry_run <- function(con, sql,
                      show = getOption("dplyr.show_sql"),
                      explain = getOption("dplyr.explain_sql")) {
    if (!is.null(show) && show) message(sql)
    if (!is.null(explain) && explain) message(qry_explain(con, sql))

    dbSendUpdate(con, sql)

    invisible(NULL)
  }

  sql_create_table <- function(con, table, types) {
    assert_that(is.string(table), is.character(types))

    field_names <- escape(ident(names(types)), collapse = NULL, con = con)
    fields <- sql_vector(paste0(field_names, " ", types,
                                ifelse(str_detect(types, "^VARCHAR"), " COLLATE Chinese_PRC_BIN", "")),
                         parens = TRUE, collapse = ", ", con = con)
    sql <- build_sql("CREATE ",
                     "TABLE ", ident(table), " ", fields, con = con)

    sql
  }

  if( temporary ) {
    name <- paste0("#", name)
  }
  if (isTRUE(db_has_table(dest$con, name))) {
    stop("Table ", name, " already exists.", call. = FALSE)
  }

  types <- types %||% db_data_type(dest$con, df)
  names(types) <- names(df)

  con <- dest$con

  if( create ) {
    qry_run(con, sql_create_table(con, name, types))
  } else {
    qry_run(con, paste0("TRUNCATE TABLE ", name))
  }
  sql_insert_into(con, name, df)
  #   sql_create_indexes(con, name, indexes)
  #   if (analyze) sql_analyze(con, name)
  #  sql_commit(con)

  tbl(dest, name)
}
