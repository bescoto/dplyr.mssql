#' Connect to SQL Server.
#' 
#' Use \code{src_sqlserver} to connect to an existing mysql or mariadb database,
#' and \code{tbl} to connect to tables within that database. 
#' 
#' @template db-info
#' @param dbname Database name
#' @param host,port Host name and port number of database
#' @param user,password User name and password (if needed)
#' @param ... for the src, other arguments passed on to the underlying
#'   database connector, \code{dbConnect}. For the tbl, included for 
#'   compatibility with the generic, but otherwise ignored.
#' @param src a sql server src created with \code{src_sqlserver}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @export
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # To connect to a database first create a src:
#' my_db <- src_mysql(host = "blah.com", user = "hadley",
#'   password = "pass")
#' # Then reference a tbl within that src
#' my_tbl <- tbl(my_db, "my_table")
#' }
#'
#' # Here we'll use the Lahman database: to create your own local copy,
#' # create a local database called "lahman", or tell lahman_mysql() how to 
#' # a database that you can write to
#' 
#' if (has_lahman("mysql")) {
#' # Methods -------------------------------------------------------------------
#' batting <- tbl(lahman_mysql(), "Batting")
#' dim(batting)
#' colnames(batting)
#' head(batting)
#'
#' # Data manipulation verbs ---------------------------------------------------
#' filter(batting, yearID > 2005, G > 130)
#' select(batting, playerID:lgID)
#' arrange(batting, playerID, desc(yearID))
#' summarise(batting, G = mean(G), n = n())
#' mutate(batting, rbi2 = 1.0 * R / AB)
#' 
#' # note that all operations are lazy: they don't do anything until you
#' # request the data, either by `print()`ing it (which shows the first ten 
#' # rows), by looking at the `head()`, or `collect()` the results locally.
#'
#' system.time(recent <- filter(batting, yearID > 2010))
#' system.time(collect(recent))
#' 
#' # Group by operations -------------------------------------------------------
#' # To perform operations by group, create a grouped object with group_by
#' players <- group_by(batting, playerID)
#' group_size(players)
#'
#' # MySQL doesn't support windowed functions, which means that only
#' # grouped summaries are really useful:
#' summarise(players, mean_g = mean(G), best_ab = max(AB))
#'
#' # When you group by multiple level, each summarise peels off one level
#' per_year <- group_by(batting, playerID, yearID)
#' stints <- summarise(per_year, stints = max(stint))
#' filter(ungroup(stints), stints > 3)
#' summarise(stints, max(stints))
#'
#' # Joins ---------------------------------------------------------------------
#' player_info <- select(tbl(lahman_mysql(), "Master"), playerID, hofID, 
#'   birthYear)
#' hof <- select(filter(tbl(lahman_mysql(), "HallOfFame"), inducted == "Y"),
#'  hofID, votedBy, category)
#' 
#' # Match players and their hall of fame data
#' inner_join(player_info, hof)
#' # Keep all players, match hof data where available
#' left_join(player_info, hof)
#' # Find only players in hof
#' semi_join(player_info, hof)
#' # Find players not in hof
#' anti_join(player_info, hof)
#'
#' # Arbitrary SQL -------------------------------------------------------------
#' # You can also provide sql as is, using the sql function:
#' batting2008 <- tbl(lahman_mysql(),
#'   sql("SELECT * FROM Batting WHERE YearID = 2008"))
#' batting2008
#' }
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
#' @rdname src_sqlserver
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
join_sql.tbl_sqlserver <- function(x, y, type, by = NULL, copy = FALSE, auto_index = FALSE,
                                   ...) {
  type <- match.arg(type, c("left", "right", "inner", "full"))
  by <- by %||% common_by(x, y)
  
  y <- auto_copy(x, y, copy, indexes = if (auto_index) list(by))
  
  # Ensure tables have unique names
  x_names <- sub(".+\\.", "", auto_names(x$select))
  y_names <- sub(".+\\.", "", auto_names(y$select))
  
  uniques <- unique_names(x_names, y_names, by, x_suffix = "_LEFT", y_suffix = "_RIGHT")
  if (!is.null(uniques)) {
    x <- update(x, select = setNames(x$select, uniques$x))
    y <- update(y, select = setNames(y$select, uniques$y))
  }

  x_names <- auto_names(x$select)
  y_names <- auto_names(y$select)
  
  vars <- lapply(
    c(
      paste0("_LEFT.", by), 
      setdiff(c(x_names, y_names), by)), 
    as.name
  )
  
  join <- switch(type, left = sql("LEFT"), inner = sql("INNER"),
                 right = stop("Right join not supported", call. = FALSE),
                 full = stop("Full join not supported", call. = FALSE))
  
  
  left <- escape(ident("_LEFT"), con = x$src$con)
  right <- escape(ident("_RIGHT"), con = x$src$con)
  
  by_escaped <- escape(ident(by), collapse = NULL, con = x$src$con)
  on <- sql(paste0(left, ".", by_escaped, " = ", right, ".", by_escaped, 
                   collapse = " AND "))
  
  from <- build_sql(from(x, "_LEFT"), "\n\n",
                    join, " JOIN \n\n" ,
                    from(y, "_RIGHT"), "\n\n",
                    "ON ", on, con = x$src$con)
  
  update(tbl(x$src, as.join(from), vars = vars), group_by = groups(x))
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
sql_insert_into.SQLServerConnection <- function(con, table, values) {
  
  MAX_INSERT_ROWS <- 1000
  qry_run <- function(con, sql, 
                      show = getOption("dplyr.show_sql"),
                      explain = getOption("dplyr.explain_sql")) {
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

#' @export
sql_commit.SQLServerConnection <- function(con) {
  TRUE
}

#' @export
copy_to.src_sqlserver <- function(dest, df, name = deparse(substitute(df)), 
                            types = NULL, temporary = TRUE, indexes = NULL, 
                            analyze = TRUE, ...) {
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
  
  qry_run(con, sql_create_table(con, name, types))
  sql_insert_into(con, name, df)
#   sql_create_indexes(con, name, indexes)
#   if (analyze) sql_analyze(con, name)
#  sql_commit(con)
  
  tbl(dest, name)
}

#' @export
qry_fields.SQLServerConnection <- function(con, from) {
  names(qry_fetch(con, paste0("SELECT TOP 0 * FROM ", from), 0L))
}

#' @export
table_fields.SQLServerConnection <- function(con, table) {
  (qry_fields(con, as.character(table)))
}

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
      p <- grep(paste0("^(\\w+\\.)?", as.character(x), "$"), as.vector(all_select))
      if(length(p)==1)
        return(as.name(all_select[[p]]))
      else
        return(x)
    })
  }
  
  if(!is.null(object$group_by)){
    object$group_by <- lapply(object$group_by, function(x){
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
  
  object$query <- build_query(object)
  object
}
