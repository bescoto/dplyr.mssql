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