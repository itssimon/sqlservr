library(sqlservr)

options('sqlservr.db_host'      = 'localhost,1433')
options('sqlservr.db_user'      = 'sa')
options('sqlservr.db_password'  = 'uKzMqA8nyj')
options('sqlservr.db_database'  = 'master')
options('sqlservr.db_collation' = 'Latin1_General_CI_AS')

context("db_split_table_name")

test_that("db_split_table_name works", {
    t <- db_split_table_name('database.schema.table')

    expect_equal(t$db, 'database')
    expect_equal(t$schema, 'schema')
    expect_equal(t$table, 'table')

    t <- db_split_table_name('schema.table')

    expect_equal(t$db, 'master')
    expect_equal(t$schema, 'schema')
    expect_equal(t$table, 'table')

    t <- db_split_table_name('table')

    expect_equal(t$db, 'master')
    expect_equal(t$schema, 'dbo')
    expect_equal(t$table, 'table')

    conn <- db_connect()
    sql <- "IF DB_ID('test') IS NULL CREATE DATABASE test"
    sqlQuery(conn, sql)
    db_close(conn)
    conn <- db_connect('test')

    t <- db_split_table_name('table', conn)

    expect_equal(t$db, 'test')
    expect_equal(t$schema, 'dbo')
    expect_equal(t$table, 'table')
})
