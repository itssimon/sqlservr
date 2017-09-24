#' Get SQL query from file
#'
#' @param file filename of file containing the SQL query
#' @param query character string referencing the SQL query in the file (in case there is more than one)
#' @param parameters list of parameters to substitute in the SQL query
#' @return SQL query as character string
#' @importFrom stringr str_match str_trim
#' @export
get_sql <- function (file, query = 1, parameters = NULL) {
    # Check arguments
    stopifnot(
        is.character(file) && length(file) == 1L && !is.na(file),
        (is.character(query) || is.numeric(query) || is.integer(query)) && length(query) == 1L && !is.na(query),
        is.null(parameters) || is.list(parameters)
    )

    # Read all lines of the file
    fileLines <- readLines(file, warn = FALSE)

    # Match the query divider lines (starting with "-- [?]", where ? is a query reference)
    divMatch <- str_match(fileLines, "^--\\s*\\[([A-Za-z0-9_\\-\\.]+)\\]")
    divPos <- which(!is.na(divMatch[, 2]))

    if (length(divPos) == 0) {
        # No query divider lines found, assuming one query
        queryLines <- fileLines
    } else {
        # Extract the query lines
        startLine <- match(query, divMatch[, 2]) + 1
        endLine <- divPos[divPos > startLine][1] - 1
        if (is.na(endLine)) endLine <- length(fileLines)
        queryLines <- fileLines[startLine:endLine]
    }

    # Prepare query lines and paste them together
    queryLines <- str_trim(sub("--.*", "", queryLines))
    queryLines <- queryLines[queryLines != ""]
    sql <- paste(queryLines, collapse = " ")

    # Replace parameters (and escape single quotes)
    for (param in names(parameters)) {
        name <- paste0("@", param, "@")
        value <- parameters[[param]]

        if (is.na(value) || is.null(value)) {
            sql <- gsub(paste0("'", name, "'"), 'NULL', sql, fixed = T)
            sql <- gsub(name, 'NULL', sql, fixed = T)
        } else {
            value <- gsub("'", "''", value, fixed = T)
            sql <- gsub(name, value, sql, fixed = T)
        }
    }

    Encoding(sql) <- "UTF-8"
    sql
}


#' Retrieves reference and name of available queries from a file
#'
#' @param file filename of file containing SQL queries
#' @importFrom stringr str_match str_trim
#' @export
get_sql_queries <- function (file) {
    # Check arguments
    stopifnot(
        is.character(file) && length(file) == 1L && !is.na(file)
    )

    # Read all lines of the file
    fileLines <- readLines(file, warn = FALSE)

    # Match the query divider lines (starting with "-- [?]", where ? is a query reference)
    divMatch <- str_match(fileLines, "^--\\s*\\[([A-Za-z0-9_\\-\\.]+)\\]\\s*(.*)")
    queries <- as.data.frame(divMatch[(which(!is.na(divMatch[, 2]))), c(2,3)])
    queries[,1] <- str_trim(queries[,1])
    queries[,2] <- str_trim(queries[,2])
    colnames(queries) <- c('query', 'name')
    queries
}


#' Execute SQL query from file
#'
#' @param file filename of file containing the SQL query
#' @param query character string referencing the SQL query in the file (in case there is more than one)
#' @param parameters list of parameters to substitute in the SQL query
#' @param conn RODBC connection object as returned by db_connect(). Optional, but allows to re-use an already open connection.
#' @return SQL query as character string
#' @importFrom RODBC sqlQuery
#' @importFrom data.table as.data.table
#' @export
db_query <- function (file, query = 1, parameters = NULL, conn = NULL) {
    sql <- get_sql(file, query, parameters)
    newConn <- is.null(conn)
    if (newConn) conn <- db_connect()
    res <- sqlQuery(conn, sql)
    if (newConn) db_close(conn)
    as.data.table(res)
}
