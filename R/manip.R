#' @export
summarise.tbl_sqlserver<- function(.data, ..., .collapse_result = TRUE) {
  input <- partial_eval(dots(...), .data, parent.frame())
  if(is.null(names(input)) || names(input) == "")
    names(input) <- paste0('"', names(auto_name(input)), '"')
  
  # Effect of previous operations on summarise:
  # * select: none
  # * filter: none, just modifies WHERE (which is applied before)
  # * mutate: need to be precomputed so new select can use
  # * arrange: intersection with new variables preserved
  if (.data$mutate) {
    .data <- collapse(.data)
  }
  
  .data$summarise <- TRUE
  
  .data <- update(.data, select = c(.data$group_by, input))
  
  if (!.collapse_result) return(.data)
  # Technically, don't always need to collapse result because summarise + filter
  # could be expressed in SQL using HAVING, but that's the only dplyr operation
  # that can be, so would be a lot of extra work for minimal gain
  update(
    collapse(.data),
    group_by = drop_last(.data$group_by)
  )
}

#' @import stringr
#' @export
copy_to.src_sqlserver <- function(dest, df, name = deparse(substitute(df)), 
                                  types = NULL, temporary = TRUE, indexes = NULL, 
                                  analyze = TRUE, create = TRUE, ...) {
  assert_that(is.data.frame(df), is.string(name), is.flag(temporary))
  
  qry_run <- function(con, sql, 
                      show = getOption("dplyr.show_sql"),
                      explain = getOption("dplyr.explain_sql")) {
    if (show) message(sql)
    if (explain) message(qry_explain(con, sql))
    
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