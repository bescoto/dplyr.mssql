#' create "src_sqlserver" object
#' @import dplyr
#' @export
src_sqlserver <- function(dbname, host = NULL, user = "root", 
                      password = "", ...) {
  if (!require("RSQLServer")) {
    stop("RSQLServer package required to connect to SQL Server", call. = FALSE)
  }
  
  drv <- SQLServer() 
  
  con <- dbConnect(drv, sprintf("jdbc:sqlserver://%s; DatabaseName=%s", host, dbname), user, password)
  info <- dbGetInfo(con)
  
  src_sql("sqlserver", con, 
          info = info, disco = function(con) dbDisconnect(con, "sqlserver"))
}

#' @export
tbl.src_sqlserver <- function(src, from, ...) {
  tbl_sql <- function(subclass, src, from, ..., vars = NULL, name = NULL) {
    assert_that(is.character(from), length(from) == 1)
    
    if (!is.sql(from)) { # Must be a character string
      if (isFALSE(db_has_table(src$con, from))) {
        stop("Table ", from, " not found in database ", src$path, call. = FALSE)
      }
      
      from <- ident(from)
    } else if (!is.join(from)) { # Must be arbitrary sql
      # Abitrary sql needs to be wrapped into a named subquery
      name <- ident(unique_name())
      from <- build_sql("(", from, ") AS ", name, con = src$con)
    }
    # init tbl_sqlserver data structure
    tbl <- make_tbl(c(subclass, "sql"),
                    src = src,              # src object
                    from = from,            # table, join, or raw sql
                    select = vars,          # SELECT: list of symbols
                    summarise = FALSE,      #   interpret select as aggreagte functions?
                    mutate = FALSE,         #   do select vars include new variables?
                    where = NULL,           # WHERE: list of calls
                    group_by = NULL,        # GROUP_BY: list of names
                    order_by = NULL,        # ORDER_BY: list of calls
                    name = name
    )
    # fill in tbl_sqlserver data structure
    update(tbl)
  }
  
  tbl_sql("sqlserver", src = src, from = from, ...)
}

#' @export
brief_desc.src_sqlserver <- function(x) {
  info <- x$info
  
  paste0("sqlserver ", info$serverVersion, " [", info$user, "@", 
         info$host, ":", info$port, "/", info$dbname, "]")
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
sql_select.SQLServerConnection <- function(con, select, from, where = NULL, group_by = NULL,
                       having = NULL, order_by = NULL, limit = NULL, 
                       offset = NULL) {
  out <- vector("list", 7)
  names(out) <- c("select", "from", "where", "group_by", "having", "order_by",
                  "offset")
  
  assert_that(is.character(select), length(select) > 0L)
  
  if (!is.null(limit)) {
    assert_that(is.integer(limit), length(limit) == 1L)
    out$select <- build_sql("SELECT TOP ", limit, " ", escape(select, collapse = ", ", con = con), con = con)
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
escape_ident.SQLServerConnection <- function(con, x) {
  as.character(x)
}



#' @export
mutate.tbl_sqlserver <- function(.data, ...) {
  input <- partial_eval(dots(...), .data, parent.frame())
  names(input) <- ifelse(names(input)=="", sql_quote(auto_name(input), '"'), names(input))
  
  .data$mutate <- TRUE
  update(.data, select = c(.data$select, input))
}

#' @export
sql_begin_trans.SQLServerConnection <- function(con) {
  "BEGIN TRANSACTION"
}



#' @export
sql_commit.SQLServerConnection <- function(con) {
  TRUE
}

#' @export
collapse.tbl_sqlserver <- function(x, vars = NULL, ...) {
  # If you collapse a query, the names of the fields will be the output names
  # of the previous query.
  if (is.null(vars)) {
    nms <- auto_names(x$select)
    if(!is.null(x$name) && x$name != "") {
      nms <- paste0(x$name, '.', nms)
    }
    vars <- lapply(nms, as.name)
  }
  
  tbl <- tbl(x$src, x$query$sql, vars = vars, ...)
  
  update(tbl, group_by = groups(x))
}

#' fill in tbl_sqlserver data structure
#' @export
update.tbl_sqlserver <- function(object, ...) {
  args <- list(...)
  assert_that(only_has_names(args, c("select", "where", "group_by", "order_by")))
  all_select <- lapply(object$select, as.character)
  for (nm in names(args)) {
    object[[nm]] <- args[[nm]]
  }
  
  if(!is.null(object$select)){
    names(object$select) <- apply(as.array(auto_names(object$select)), 
                                  1, function(x) {
                                    sub("^\\w+\\.", "", as.character(x))
                                  })
    
    object$select <- lapply(object$select, function(x){
      if(!is.name(x) && !is.character(x)) {
        return(x)
      }
      p <- grep(paste0("^(\\w+\\.)?", as.character(x), "$"), as.vector(all_select))
      if(length(p)==1)
        return(as.name(all_select[[p]]))
      else
        return(x)
    })
  }
  
  if(!is.null(object$group_by)){
    object$group_by <- lapply(object$group_by, function(x){
      if(!is.name(x) && !is.character(x)) {
        return(x)
      }
      p <- grep(paste0("^(\\w+\\.)?", as.character(x), "$"), as.vector(all_select))
      if(length(p)==1)
        return(as.name(all_select[[p]]))
      else
        return(x)
    })
  }
  
  # Figure out variables
  if (is.null(object$select)) {
    if (is.ident(object$from)) {
      var_names <- table_fields(object$src$con, object$from)
    } else {
      var_names <- qry_fields(object$src$con, object$from)
    }
    vars <- lapply(var_names, as.name)
    object$select <- vars
  }
  
  NextMethod("update", object, select = object$select, where = object$where, 
             group_by = object$group_by, order_by = object$order_by)
}


#' @export
as.data.frame.tbl_sqlserver <- function (x, row.names = NULL, optional = FALSE, ..., n = -1L) {
#   res <- dbSendQuery(con, sql)
#   on.exit(dbClearResult(res))
#   out <- fetch(res, n)
#   res_warn_incomplete(res)
#   out
  x$query$fetch(n)
}
