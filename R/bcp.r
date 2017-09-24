#' Write data.frame to database using bcp
#'
#' @param data data.frame to be written to database. Column names must match with those in the database table.
#' @param table character string naming the table to write to. Can include database and schema name (database_name.schema_name.table_name).
#' @param conn RODBC connection object as returned by db_connect(). Optional, but allows to re-use an already open connection to check columns and truncate table (if \code{truncate} is \code{TRUE}).
#' @param truncate logical. If \code{TRUE}, the table will be truncated prior to writing data.
#' @param preserve_empty_strings logical. If \code{TRUE}, empty strings in the data will be preserved by importing a dummy string and then updating the table afterwards. Otherwise empty strings will be imported as NULLs.
#' @param sep character string to use as column separater when writing the data file passed to the bcp utility. Make sure this string does not exist in the data.
#' @param eol character string to use as end of line separater when writing the data file passed to the bcp utility. Make sure this string does not exist in the data.
#' @param paranoid logical. If \code{TRUE}, existing separator and EOL strings will be removed in the data. This may be a bit slow, so it's better to choose parameters \code{sep} and \code{eol} carefully.
#' @param preserve_memory logical. If \code{TRUE} and a data.table is passed as \code{data}, the object will not be copied in memory. The original data.table may then be changed by this function.
#' @param tmp_dir character string
#' @param tmp_keep_files logical
#' @import data.table
#' @importFrom RODBC sqlColumns sqlClear
#' @importFrom gdata write.fwf
#' @importFrom utils write.table
#' @export
db_bcp <- function (data, table, conn = NULL, truncate = FALSE, preserve_empty_strings = TRUE,
                    sep = "^|~", eol = "^|\r\n", paranoid = FALSE,
                    preserve_memory = FALSE, tmp_dir = ".", tmp_keep_files = FALSE) {

    # Check arguments
    stopifnot(
        is.data.frame(data),
        is.null(conn) || 'RODBC' %in% class(conn),
        is.character(table) && length(table) == 1L && !is.na(table),
        is.logical(truncate) && length(truncate) == 1L && !is.na(truncate),
        is.logical(preserve_empty_strings) && length(preserve_empty_strings) == 1L && !is.na(preserve_empty_strings),
        is.character(sep) && length(sep) == 1L && !is.na(sep),
        is.character(eol) && length(eol) == 1L && !is.na(eol),
        is.logical(paranoid) && length(paranoid) == 1L && !is.na(paranoid),
        is.logical(preserve_memory) && length(preserve_memory) == 1L && !is.na(preserve_memory),
        is.character(tmp_dir) && length(tmp_dir) == 1L && !is.na(tmp_dir),
        is.logical(tmp_keep_files) && length(tmp_keep_files) == 1L && !is.na(tmp_keep_files)
    )

    if (nrow(data) > 0) {

        cat_msg('Preparing data for import...')
        start_time <- Sys.time()

        # Connect to database if necessary
        newConn <- is.null(conn)
        t <- db_split_table_name(table)
        if (newConn) {
            conn <- db_connect(t$db)
        }

        # Get table information
        tableName <- paste(t$schema, t$table, sep = '.')
        tableColumns <- sqlColumns(conn, tableName)

        # Check columns
        if (!all(colnames(data) %in% tableColumns$COLUMN_NAME)) {
            if (newConn) {
                db_close(conn)
            }
            stop('Columns of data do not match table columns')
        }

        # Truncate table
        if (truncate) {
            sqlClear(conn, tableName)
        }

        # Get collation
        collation <- getOption('sqlservr.db_collation')
        if (is.null(collation)) {
            collation <- db_get_collation(conn)
            options('sqlservr.db_collation' = collation)
        }

        # Prepare data
        if (!is.data.table(data)) {
            data <- as.data.table(data)
        } else if (!preserve_memory) {
            data <- copy(data)
        }

        if (preserve_empty_strings) {
            empty_string <- getOption('sqlservr.empty_string')
            col_has_empty_strings <- list()
            if (is.null(empty_string)) {
                empty_string <- '[[EmptyString]]'
            }
        }

        for (col in colnames(data)) {
            if (is.factor(data[[col]])) {
                # Convert factors to character strings
                set(data, j = col, value = as.character(data[[col]]))
            } else if (is.logical(data[[col]])) {
                # Convert logicals to 0/1
                set(data, j = col, value = as.integer(data[[col]]))
            }
            if (is.character(data[[col]])) {
                if (paranoid) {
                    # Make sure the separator and EOL strings do not exist in data
                    set(data, j = col, value = gsub(sep, "",     data[[col]], fixed = T))
                    set(data, j = col, value = gsub(eol, "\r\n", data[[col]], fixed = T))
                }
                if (preserve_empty_strings && any(data[[col]] == '', na.rm = TRUE)) {
                    # Replace empty strings with a dummy string
                    set(data, i = which(data[[col]] == ''), j = col, value = empty_string)
                    col_has_empty_strings[[col]] <- TRUE
                }
            }
        }

        # Prepare file names and create temp directory (if necessary)
        rnd <- paste(sample(c(0:9, letters), 5, replace = T), collapse = "")
        fileDat <- file.path(tmp_dir, paste(t$db, t$schema, t$table, "tmp", rnd, "dat", sep = "."))
        fileFml <- file.path(tmp_dir, paste(t$db, t$schema, t$table, "tmp", rnd, "fml", sep = "."))
        dir.create(tmp_dir, showWarnings = FALSE, recursive = TRUE)

        # Disable scientifc notation
        scipen <- getOption('scipen')
        outdec <- getOption('OutDec')
        options(scipen = 999)
        options(OutDec = '.')

        # Write data file
        # Can't use fwrite()'s sep argument because it doesn't accept multiple characters,
        # so we're concatenating columns here
        fwrite(data.table(do.call(paste_omit_na, c(data, list(sep = sep)))),
               fileDat, quote = F, eol = eol, na = "", col.names = F)

        # Write format file
        n <- ncol(data)
        format <- data.table(as.character(1:n),
                             rep("SQLCHAR", n),
                             rep("0", n),
                             rep("0", n),
                             paste0('"', c(rep(sep, n - 1), gsub("\r\n", "\\r\\n", eol, fixed = TRUE)), '"'),
                             as.character(match(colnames(data), tableColumns$COLUMN_NAME)),
                             colnames(data),
                             rep(collation, n))
        writeLines(c("10.0", n), fileFml)
        gdata::write.fwf(format, fileFml, append = TRUE, colnames = FALSE, justify = "left",
                         width = c(8, 20, 8, 8, 11, 6, 27, 28))

        # Reset scientific notation to original state
        options('scipen' = scipen)
        options('OutDec' = outdec)

        # Generate bcp command
        cmd <- paste0('bcp ', tableName, ' in ', fileDat, ' -f ', fileFml, ' -d "', t$db, '"',
                      ' -S "', getOption('sqlservr.db_host'), '" -a 65535 -k')
        if (!is.null(getOption('sqlservr.db_user'))) {
            # Use SQL Server authentication (username and password)
            cmd <- paste0(cmd, ' -U "', getOption('sqlservr.db_user'), '" -P "', getOption('sqlservr.db_password'), '"')
        } else {
            # Use Windows authentication (trusted connection)
            cmd <- paste0(cmd, ' -T')
        }

        prep_time <- difftime(Sys.time(), start_time, units = 'secs')
        cat_msg('Finished preparation in %.1f sec', prep_time)

        # Run bcp command
        status <- system(cmd)

        if (status == 0) {
            # Delete data and format files on success
            if (!tmp_keep_files) {
                file.remove(fileDat)
                file.remove(fileFml)
            }

            if (preserve_empty_strings) {
                cat_msg('Updating table to restore empty strings...')
                sqls <- sapply(names(col_has_empty_strings), function (x) { sprintf("UPDATE %s SET %s = '' WHERE %s = '%s'", tableName, x, x, empty_string, x) })
                for (sql in sqls) {
                    sqlQuery(conn, sql)
                }
            }
        } else {
            warning('bcp command failed. Temporary data and format files were not deleted.')
        }

        # Close database connection
        if (newConn) {
            db_close(conn)
        }
    } else {
        warning('No data was loaded. The data.frame contained zero rows.')
    }
    invisible()
}
