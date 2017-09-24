db_get_database <- function (conn) {
    cs <- attr(conn, 'connection.string')
    if (!is.null(cs)) {
        cs <- strsplit(cs, ";", fixed = TRUE)[[1]]
        i <- which(grepl("^database=", cs, ignore.case = TRUE))
        cs <- strsplit(cs, "=", fixed = TRUE)
        cs[[i]][2]
    } else {
        NA
    }
}


#' @importFrom RODBC sqlQuery
db_get_schema <- function (conn) {
    res <- sqlQuery(conn, "SELECT SCHEMA_NAME()")
    as.character(res[[1,1]])
}


#' @importFrom RODBC sqlQuery
db_get_collation <- function (conn) {
    res <- sqlQuery(conn, "SELECT CONVERT(NVARCHAR, SERVERPROPERTY('collation'))")
    as.character(res[[1,1]])
}


#' @importFrom stringr str_trim str_split
db_split_table_name <- function (table, conn = NULL) {
    # Split table name by dots
    t <- str_trim(str_split(table, "\\.")[[1]])
    # Remove brackets and reverse order
    t <- rev(str_trim(sub("^\\[(.+)\\]$", "\\1", t)))
    # Set default values if necessary
    if (is.na(t[3]) && !is.null(conn)) t[3] <- db_get_database(conn)
    if (is.na(t[3])) t[3] <- getOption('sqlservr.db_database')
    if (is.na(t[2]) && !is.null(conn)) t[2] <- db_get_schema(conn)
    if (is.na(t[2])) t[2] <- 'dbo'

    list(table = t[1], schema = t[2], db = t[3])
}


paste_omit_na <- function (..., sep = " ") {
    L <- list(...)
    L <- lapply(L, function(x) { x[is.na(x)] <- ""; x })
    do.call(paste, c(L, list(sep = sep)))
}


cat_msg <- function (string, ...) {
    cat(sprintf(paste0(string, '\n'), ...))
}
