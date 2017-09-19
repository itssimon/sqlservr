suppressMessages(library(RODBC))
suppressMessages(library(stringr))
suppressMessages(library(gdata, warn.conflicts = FALSE, quietly = TRUE))


options('sqlservr.db_host' = '10.203.185.31,1433')
options('sqlservr.db_user' = 'review')
options('sqlservr.db_password' = '#Password1')
options('sqlservr.db_database' = 'TfNSW_2017_Toll')
options('sqlservr.db_schema' = 'Staging')


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


db_get_database <- function (conn) {
    c <- attr(conn, 'connection.string')
    if (!is.null(c)) {
        c <- strsplit(c, ";", fixed = TRUE)[[1]]
        i <- which(grepl("^database=", c, ignore.case = TRUE))
        c <- strsplit(c, "=", fixed = TRUE)
        c[[i]][2]
    } else {
        NULL
    }
}


#' @importFrom RODBC sqlQuery
db_get_collation <- function (conn) {
    res <- sqlQuery(conn, "SELECT CONVERT(NVARCHAR, SERVERPROPERTY('collation'))")
    as.character(res[[1,1]])
}


#' @importFrom RODBC sqlQuery
db_get_schema <- function (conn) {
    res <- sqlQuery(conn, "SELECT SCHEMA_NAME()")
    as.character(res[[1,1]])
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

#' Write data.frame to database using bcp
#'
#' @param data data.frame to be written to database. Column names must match with those in the database table.
#' @param table character string naming the table to write to. Can include database and schema name (database_name.schema_name.table_name).
#' @param conn RODBC connection object as returned by db_connect(). Optional, but allows to re-use an already open connection to check columns and truncate table (if \code{truncate} is \code{TRUE}).
#' @param truncate logical. If \code{TRUE}, the table will be truncated prior to writing data.
#' @param tmpSuffix character string
#' @param tmpDir character string
#' @importFrom RODBC sqlColumns sqlClear
#' @importFrom gdata write.fwf
#' @importFrom utils write.table
#' @export
db_bcp <- function (data, table, conn = NULL, truncate = FALSE, tmpSuffix = "tmp", tmpDir = ".") {
    # Check data
    if (!is.null(data) && is.data.frame(data) && nrow(data) > 0) {

        # Prepare data
        sep <- "^|^"
        eol <- "^|\r\n"
        data[data == ''] <- rawToChar(as.raw(0))  # fix for future R versions
        data <- as.data.frame(lapply(data, function(x) {
            if (is.factor(x)) x <- as.character(x)
            if (is.character(x)) {
                x <- gsub(sep, "", x, fixed = T)
                x <- gsub(eol, "", x, fixed = T)
            }
            x
        }))

        # Connect to database if necessary
        newConn <- is.null(conn)
        t <- db_split_table_name(table, conn)
        if (newConn) conn <- db_connect(t$db)

        # Get table information
        tableName <- paste(t$schema, t$table, sep = '.')
        tableColumns <- sqlColumns(conn, tableName)

        # Check columns
        if (!all(colnames(data) %in% tableColumns$COLUMN_NAME)) {
            stop('Columns of data do not match table columns')
        }

        # Truncate table
        if (truncate) {
            sqlClear(conn, tableName)
        }

        # Get collation
        collation <- db_get_collation(conn)

        # Close database connection
        if (newConn) db_close(conn)

        # Prepare file names and create temp directory (if necessary)
        rnd <- paste(sample(c(0:9, letters), 5, replace = T), collapse = "")
        fileDat <- file.path(tmpDir, paste(t$db, t$schema, t$table, tmpSuffix, rnd, "dat", sep = "."))
        fileFml <- file.path(tmpDir, paste(t$db, t$schema, t$table, tmpSuffix, rnd, "fml", sep = "."))
        dir.create(tmpDir, showWarnings = FALSE, recursive = TRUE)

        # Write data file
        fileConn <- file(fileDat, "wb")
        write.table(data, fileConn, quote = F, sep = sep, eol = eol, na = "", dec = ".",
                    row.names = F, col.names = F, fileEncoding = "UTF-8")
        close(fileConn)

        # Write format file
        n <- ncol(data)
        format <- data.frame(as.character(1:n), rep("SQLCHAR", n), rep("0", n), rep("0", n),
                             paste0('"', c(rep(sep, n-1), gsub("\r\n", "\\r\\n", eol, fixed = TRUE)), '"'),
                             as.character(match(colnames(data), tableColumns$COLUMN_NAME)),
                             colnames(data), rep(collation, n))
        writeLines(c("10.0", n), fileFml)
        gdata::write.fwf(format, fileFml, append = TRUE, colnames = FALSE, justify = "left",
                         width = c(8, 20, 8, 8, 11, 6, 27, 28))

        # Execute bcp
        cmd <- paste0('bcp ', tableName, ' in ', fileDat, ' -f ', fileFml, ' -d "', t$db, '"',
                      ' -S "', getOption('sqlservr.db_host'), '"')
        if (!is.null(getOption('sqlservr.db_user'))) {
            cmd <- paste0(cmd, ' -U "', getOption('sqlservr.db_user'), '" -P "', getOption('sqlservr.db_password'), '"')
        } else {
            cmd <- paste0(cmd, ' -T')
        }
        status <- system(cmd)

        # Delete files on success
        if (status == 0) {
            file.remove(fileDat)
            file.remove(fileFml)
        } else {
            warning('bcp command failed. Data and format temporary files were not deleted.')
        }
    } else {
        warning('No data was loaded. The data passed was either empty or not a data.frame.')
    }
}


#' @importFrom stringr str_trim str_split
db_split_table_name <- function (table, conn) {
    # Split table name by dots
    t <- str_trim(str_split(table, "\\.")[[1]])
    # Remove brackets and reverse order
    t <- rev(str_trim(sub("^\\[(.+)\\]$", "\\1", t)))
    # Set default values if necessary
    if (is.na(t[3])) t[3] <- db_get_database(conn)
    if (is.na(t[2])) t[2] <- db_get_schema(conn)

    list(table = t[1], schema = t[2], db = t[3])
}


#' Get SQL query from file
#'
#' @param file filename of file containing the SQL query
#' @param query character string referencing the SQL query in the file (in case there is more than one)
#' @param parameters list of parameters to substitute in the SQL query
#' @return SQL query as character string
#' @importFrom stringr str_match str_trim
#' @export
get_sql <- function (file, query = 1, parameters = NULL) {
    # Read all lines of the file
    fileLines <- readLines(file, warn = FALSE)

    # Match the query divider lines (starting with "-- [?]", where ? is a number)
    divMatch <- str_match(fileLines, "^-- \\[([A-Za-z0-9_-]+)\\]")
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
    # Read all lines of the file
    fileLines <- readLines(file, warn = FALSE)

    # Match the query divider lines (starting with "-- [?]", where ? is a number)
    divMatch <- str_match(fileLines, "^-- \\[([0-9]+)\\](.*)")
    queries <- as.data.frame(divMatch[(which(!is.na(divMatch[, 2]))), c(2,3)])
    queries[,1] <- str_trim(queries[,1])
    queries[,2] <- str_trim(queries[,2])
    colnames(queries) <- c('query', 'name')
    queries
}
