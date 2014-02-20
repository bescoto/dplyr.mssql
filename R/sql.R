sql_vector <- function(x, parens = NA, collapse = " ", con = NULL) {
  if (is.na(parens)) {
    parens <- length(x) > 1L
  }
  
  x <- names_to_as(x, con = con)
  x <- paste(x, collapse = collapse)
  if (parens) x <- paste0("(", x, ")")
  sql(x)
}

names_to_as <- function(x, con = NULL) {
  names <- names2(x)
  as <- ifelse(names == '', '', paste0(' AS ', escape_ident(con, names)))
  
  paste0(x, as)
}