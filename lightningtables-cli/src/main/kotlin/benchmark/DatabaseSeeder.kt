package com.coderjoe.benchmark

import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import kotlin.random.Random

class DatabaseSeeder {
    private val types = listOf("CREDIT", "DEBIT")
    private val services = listOf("CALL", "SMS", "DATA")

    fun seed(recordCount: Int) {
        transaction {
            val conn = this.connection.connection as java.sql.Connection

            conn.prepareStatement(
                "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, ?, ?, ?)"
            ).use { stmt ->
                repeat(recordCount) {
                    stmt.setInt(1, Random.nextInt(1, 4))
                    stmt.setString(2, types.random())
                    stmt.setString(3, services.random())
                    stmt.setDouble(4, Random.nextDouble(0.01, 10.0))
                    stmt.addBatch()
                }
                stmt.executeBatch()
            }
        }
    }
}
