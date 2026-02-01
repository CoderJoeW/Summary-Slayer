package com.coderjoe.lightingtable.core.database

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.v1.jdbc.Database

object DatabaseConfig {
    fun initialize(
        url: String,
        username: String,
        password: String,
    ) {
        val config = HikariConfig().apply {
            jdbcUrl = url
            driverClassName = "org.mariadb.jdbc.Driver"
            this.username = username
            this.password = password
            maximumPoolSize = 10
            addDataSourceProperty("useBulkStmts", "false")
        }
        Database.Companion.connect(HikariDataSource(config))
    }
}
