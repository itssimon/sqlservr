library(sqlservr)
library(data.table)
library(RODBC)

options(stringsAsFactors = FALSE)
options('sqlservr.db_host'      = 'localhost,1433')
options('sqlservr.db_user'      = 'sa')
options('sqlservr.db_password'  = 'uKzMqA8nyj')
options('sqlservr.db_database'  = 'master')
options('sqlservr.db_collation' = 'Latin1_General_CI_AS')

data <- data.table(
    CharColumn = c('Foo bar', 'Foo\r\nbar', 'Stadt Köln', 'Barriga llena, corazón contento', ' ^|', 'A^|~B', '', NA),
    NumColumn = c(10000000, 3.123454, -124, 3e3, 0, 0.0000001, 1, NA),
    IntColumn = c(10000000L, 3L, -123L, 3e3L, 0L, 0L, 0L, NA),
    BoolColumn = c(T, T, F, F, F, T, F, NA)
)

conn <- db_connect()
sql <- "IF DB_ID('test') IS NULL CREATE DATABASE test"
sqlQuery(conn, sql)
db_close(conn)

conn <- db_connect('test')
sql <- "IF OBJECT_ID('test') IS NOT NULL DROP TABLE test"
sqlQuery(conn, sql)
sql <- "CREATE TABLE test (CharColumn NVARCHAR(255), NumColumn FLOAT, IntColumn INT, BoolColumn BIT)"
sqlQuery(conn, sql)
db_bcp(data, 'test.dbo.test', truncate = T, paranoid = T, preserve_empty_strings = T)
sql <- "SELECT * FROM test.dbo.test"
res <- as.data.table(sqlQuery(conn, sql))
db_close(conn)

context("db_bcp")

test_that("db_bcp works", {
    expect_equal(nrow(res), nrow(data))

    for (i in 1:7) {
        if (i != 6) {
            expect_equal(res[[i, 'CharColumn']], data[[i, 'CharColumn']])
        }
        expect_equal(res[[i, 'NumColumn']], data[[i, 'NumColumn']])
        expect_equal(res[[i, 'IntColumn']], data[[i, 'IntColumn']])
        expect_equal(res[[i, 'BoolColumn']], as.integer(data[[i, 'BoolColumn']]))
    }

    expect_equal(res[[6, 'CharColumn']], 'AB')
    expect_equal(is.na(res[[7, 'CharColumn']]), FALSE)
    expect_equal(is.na(res[[8, 'CharColumn']]), TRUE)
    expect_equal(is.na(res[[8, 'NumColumn']]), TRUE)
    expect_equal(is.na(res[[8, 'IntColumn']]), TRUE)
    expect_equal(is.na(res[[8, 'BoolColumn']]), TRUE)
})
