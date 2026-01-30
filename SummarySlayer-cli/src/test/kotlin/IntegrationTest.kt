package com.coderjoe
import com.coderjoe.database.TransactionsRepository
import com.coderjoe.database.TransactionsTable
import com.coderjoe.database.UsersTable
import com.coderjoe.database.seeders.TransactionsSeeder
import org.jetbrains.exposed.v1.jdbc.deleteAll
import com.coderjoe.services.SummaryTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IntegrationTest : DockerComposeTestBase() {
    val parser = SummaryTriggerGeneratorSqlParser()

    @Test
    fun `sanity check - can query seeded user data`() {
        val row = transaction {
            UsersTable.selectAll()
                .where { UsersTable.firstName eq "John" }
                .single()
        }

        assertEquals("John", row[UsersTable.firstName])
        assertEquals("Doe", row[UsersTable.lastName])
    }

    @Test
    fun `creating summary table has valid structure`() {
        val result = parser.generate(queries["sumCostByUser"]!!)

        connect().use { conn ->
            conn.createStatement().execute(result.summaryTable)

            val metadata = conn.metaData
            val tables = metadata.getTables(null, null, "%summary%", arrayOf("TABLE"))
            assertTrue(tables.next(), "Summary table should be created")
            val tableName = tables.getString("TABLE_NAME")

            val specs = conn.getColumnSpecs(tableName)

            val expected =
                mapOf(
                    "user_id" to ColumnSpec(typeName = "INT", size = 10, decimalDigits = 0, nullable = false),
                    "total_cost" to ColumnSpec(typeName = "DECIMAL", size = 10, decimalDigits = 2, nullable = false),
                )

            assertEquals(expected, specs, "Column specs should match")

            val primaryKeys = metadata.getPrimaryKeys(null, null, tableName)
            assertTrue(primaryKeys.next(), "Table should have a primary key")
            assertEquals("user_id", primaryKeys.getString("COLUMN_NAME"), "Primary key should be on user_id")
        }
    }

    @Test
    fun `all three triggers are created successfully`() {
        val result = parser.generate(queries["sumCostByUser"]!!)

        transaction {
            exec(result.summaryTable)
            result.triggers.values.forEach{ exec(it)}
        }

        connect().use { conn ->
            val triggerQuery =
                """
                SELECT TRIGGER_NAME, EVENT_MANIPULATION 
                FROM INFORMATION_SCHEMA.TRIGGERS 
                WHERE TRIGGER_SCHEMA = DATABASE() 
                AND EVENT_OBJECT_TABLE = 'transactions'
                ORDER BY TRIGGER_NAME
                """.trimIndent()

            val triggerResult = conn.createStatement().executeQuery(triggerQuery)
            val triggers = mutableMapOf<String, String>()

            while (triggerResult.next()) {
                val name = triggerResult.getString("TRIGGER_NAME")
                val event = triggerResult.getString("EVENT_MANIPULATION")
                triggers[name] = event
            }

            assertEquals(3, triggers.size, "Should have exactly 3 triggers")

            assertTrue(
                triggers.any { it.key.contains("insert", ignoreCase = true) && it.value == "INSERT" },
                "Should have an INSERT trigger",
            )

            assertTrue(
                triggers.any { it.key.contains("update", ignoreCase = true) && it.value == "UPDATE" },
                "Should have an UPDATE trigger",
            )

            assertTrue(
                triggers.any { it.key.contains("delete", ignoreCase = true) && it.value == "DELETE" },
                "Should have a DELETE trigger",
            )
        }
    }

    @Test
    fun `original table and summary table match after a single insert`() {
        val result = parser.generate(queries["sumCostByUser"]!!)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.summaryTable)
            result.triggers.values.forEach { exec(it) }
        }

        TransactionsSeeder().seed(1)

        connect().use { conn ->
            val originalTableQuery =
                conn.createStatement()
                    .executeQuery(queries["sumCostByUser"]!!)

            val summaryTableQuery =
                conn.createStatement()
                    .executeQuery("SELECT * FROM transactions_user_id_summary")

            while (originalTableQuery.next()) {
                summaryTableQuery.next()
                val originalUserId = originalTableQuery.getString("user_id")
                val originalCost = originalTableQuery.getString("total_cost")
                val summaryUserId = summaryTableQuery.getString("user_id")
                val summaryTotalCost = summaryTableQuery.getString("total_cost")

                assertEquals(originalUserId, summaryUserId)
                assertEquals(originalCost, summaryTotalCost)
            }
        }
    }

    @Test
    fun `original table and summary table match after a single delete`() {
        val result = parser.generate(queries["sumCostByUser"]!!)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.summaryTable)
            result.triggers.values.forEach { exec(it) }
        }

        TransactionsSeeder().seed(10)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }

        connect().use { conn ->
            val originalTableQuery =
                conn.createStatement()
                    .executeQuery(queries["sumCostByUser"]!!)

            val summaryTableQuery =
                conn.createStatement()
                    .executeQuery("SELECT * FROM transactions_user_id_summary")

            while (originalTableQuery.next()) {
                summaryTableQuery.next()
                val originalUserId = originalTableQuery.getString("user_id")
                val originalCost = originalTableQuery.getString("total_cost")
                val summaryUserId = summaryTableQuery.getString("user_id")
                val summaryTotalCost = summaryTableQuery.getString("total_cost")

                assertEquals(originalUserId, summaryUserId)
                assertEquals(originalCost, summaryTotalCost)
            }
        }
    }

    @Test
    fun `original table and summary table match after a single update`() {
        val result = parser.generate(queries["sumCostByUser"]!!)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.summaryTable)
            result.triggers.values.forEach { exec(it) }
        }

        TransactionsSeeder().seed(10)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }, limit = 1) {
                it[cost] = 50.0
            }
        }

        connect().use { conn ->
            val originalTableQuery =
                conn.createStatement()
                    .executeQuery(queries["sumCostByUser"]!!)

            val summaryTableQuery =
                conn.createStatement()
                    .executeQuery("SELECT * FROM transactions_user_id_summary")

            while (originalTableQuery.next()) {
                summaryTableQuery.next()
                val originalUserId = originalTableQuery.getString("user_id")
                val originalCost = originalTableQuery.getString("total_cost")
                val summaryUserId = summaryTableQuery.getString("user_id")
                val summaryTotalCost = summaryTableQuery.getString("total_cost")

                assertEquals(originalUserId, summaryUserId)
                assertEquals(originalCost, summaryTotalCost)
            }
        }
    }
}
