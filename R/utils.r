dots <- function(...) {
  eval(substitute(alist(...)))
}

named_dots <- function(...) {
  auto_name(dots(...))
}

auto_names <- function(x) {
  nms <- names2(x)
  missing <- nms == ""
  if (all(!missing)) return(nms)
  
  deparse2 <- function(x) paste(deparse(x, 500L), collapse = "")
  defaults <- vapply(x[missing], deparse2, character(1), USE.NAMES = FALSE)
  
  nms[missing] <- defaults
  nms
}

auto_name <- function(x) {
  names(x) <- auto_names(x)
  x
}

all_names <- function(x) {
  if (is.name(x)) return(as.character(x))
  if (!is.call(x)) return(NULL)
  
  unique(unlist(lapply(x[-1], all_names), use.names = FALSE))
}

is.lang <- function(x) {
  is.name(x) || is.atomic(x) || is.call(x)
}

is.lang.list <- function(x) {
  if (is.null(x)) return(TRUE)
  
  is.list(x) && all_apply(x, is.lang)
}

on_failure(is.lang.list) <- function(call, env) {
  paste0(call$x, " is not a list containing only names, calls and atomic vectors")
}

only_has_names <- function(x, nms) {
  all(names(x) %in% nms)
}

#' @import assertthat
on_failure(all_names) <- function(call, env) {
  x_nms <- names(eval(call$x, env))
  nms <- eval(call$nms, env)
  extra <- setdiff(x_nms, nms)
  
  paste0(call$x, " has named components: ", paste0(extra, collapse = ", "), ".", 
         "Should only have names: ", paste0(nms, collapse = ","))
}

all_apply <- function(xs, f) {
  for (x in xs) {
    if (!f(x)) return(FALSE)
  }
  TRUE
}
any_apply <- function(xs, f) {
  for (x in xs) {
    if (f(x)) return(TRUE)
  }
  FALSE
}

drop_last <- function(x) {
  if (length(x) <= 1L) return(NULL)
  x[-length(x)]
}

compact <- function(x) Filter(Negate(is.null), x)

names2 <- function(x) {
  names(x) %||% rep("", length(x))
}

"%||%" <- function(x, y) if(is.null(x)) y else x

is.wholenumber <- function(x, tol = .Machine$double.eps ^ 0.5) {
  abs(x - round(x)) < tol
}

as_df <- function(x) {
  class(x) <- "data.frame"
  attr(x, "row.names") <- c(NA_integer_, -length(x[[1]]))
  
  x
}

deparse_all <- function(x) {
  deparse2 <- function(x) paste(deparse(x, width.cutoff = 500L), collapse = "")
  vapply(x, deparse2, FUN.VALUE = character(1))
}

commas <- function(...) paste0(..., collapse = ", ")

in_travis <- function() identical(Sys.getenv("TRAVIS"), "true")

named <- function(...) {
  x <- c(...)
  
  missing_names <- names2(x) == ""
  names(x)[missing_names] <- x[missing_names]
  
  x
}

unique_name <- local({
  i <- 0
  
  function() {
    i <<- i + 1
    paste0("_W", i)
  }
})

isFALSE <- function(x) identical(x, FALSE)

is.sql <- function(x) inherits(x, "sql")

is.join <- function(x) {
  inherits(x, "join")
}

auto_copy <- function(x, y, copy = FALSE, ...) {
  if (same_src(x, y)) return(y)
  
  if (!copy) {
    stop("x and y don't share the same src. Set copy = TRUE to copy y into ",
         "x's source (this may be time consuming).", call. = FALSE)
  }
  
  copy_to(x$src, as.data.frame(y), random_table_name(), ...)
}

random_table_name <- function(n = 10) {
  paste0(sample(letters, n, replace = TRUE), collapse = "")
}

