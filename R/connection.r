db_conn_str <- function (db = getOption('sqlservr.db_database')) {
    s <- paste0("Driver={SQL Server};Server=", getOption('sqlservr.db_host'), ";AutoTranslate=yes")

    if (!is.null(db)) {
        s <- paste0(s, ";Database=", db)
    }

    if (is.null(getOption('sqlservr.db_user'))) {
        s <- paste0(s, ";Trusted_Connection=true")
    } else {
        s <- paste0(s, ";Uid={", getOption('sqlservr.db_user'), "};Pwd={", getOption('sqlservr.db_password'), "}")
    }

    s
}


#' Open database connection.
#'
#' Connection parameters are read from options. Database can be overriden by passing the \code{db} parameter.
#' @param db Name of database to connect to
#' @return RODBC connection object
#' @importFrom RODBC odbcDriverConnect
#' @export
db_connect <- function (db = getOption('sqlservr.db_database')) {
    odbcDriverConnect(db_conn_str(db))
}


#' Close database connection
#'
#' @param conn RODBC connection object as returned by db_connect()
#' @importFrom RODBC odbcClose
#' @export
db_close <- function (conn) {
    odbcClose(conn)
    invisible()
}
