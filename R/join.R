as.join <- function(x) {
  structure(x, class = c("join", class(x)))
}

unique_names <- function(x_names, y_names, by, x_suffix = ".x", y_suffix = ".y") {
  common <- setdiff(intersect(x_names, y_names), by)
  if (length(common) == 0) return(NULL)
  
  x_match <- match(common, x_names)
  x_names[x_match] <- paste0(x_names[x_match], x_suffix)
  
  y_match <- match(common, y_names)
  y_names[y_match] <- paste0(y_names[y_match], y_suffix)
  
  list(x = x_names, y = y_names)
}

from <- function(x, name = random_table_name()) {
  build_sql("(", x$query$sql, ") AS ", ident(name), con = x$src$con)
}

join_sql <- function(x, y, type, by = NULL, copy = FALSE, auto_index = FALSE,
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
inner_join.tbl_sqlserver <- function(x, y, by = NULL, copy = FALSE,
                               auto_index = FALSE, ...) {
  join_sql(x, y, "inner", by = by, copy = copy, auto_index = auto_index, ...)
}

#' @export
left_join.tbl_sqlserver <- function(x, y, by = NULL, copy = FALSE,
                              auto_index = FALSE, ...) {
  join_sql(x, y, "left", by = by, copy = copy, auto_index = auto_index, ...)
}