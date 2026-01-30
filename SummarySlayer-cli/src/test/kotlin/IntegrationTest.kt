package com.coderjoe
import com.coderjoe.database.seeders.Transactions
import com.coderjoe.services.SummaryTriggerGeneratorSqlParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull
import kotlin.use

class IntegrationTest: DockerComposeTestBase() {
    val query =
        """
        SELECT user_id, SUM(cost) as total_cost
        FROM transactions
        GROUP BY user_id
        """.trimIndent()

    val parser = SummaryTriggerGeneratorSqlParser()

    @Test
    fun `sanity check - can query seeded user data`() {
        connect().use { conn ->
            val result = conn.createStatement()
                .executeQuery("SELECT first_name, last_name FROM users WHERE first_name = 'John'")

            assertTrue(result.next(), "Should find John Doe in seeded data")
            assertEquals("John", result.getString("first_name"))
            assertEquals("Doe", result.getString("last_name"))
        }
    }

    @Test
    fun `creating summary table has valid structure`() {
        val result = parser.generate(query)

        connect().use { conn ->
            conn.createStatement().execute(result.summaryTable)

            val metadata = conn.metaData
            val tables = metadata.getTables(null, null, "%summary%", arrayOf("TABLE"))
            assertTrue(tables.next(), "Summary table should be created")
            val tableName = tables.getString("TABLE_NAME")

            val specs = conn.getColumnSpecs(tableName)

            val expected = mapOf(
                "user_id" to ColumnSpec(typeName = "INT", size = 10, decimalDigits = 0, nullable = false),
                "total_cost" to ColumnSpec(typeName = "DECIMAL", size = 10, decimalDigits = 2, nullable = false)
            )

            for ((colName, expectedSpec) in expected) {
                val actual = specs[colName]
                assertNotNull(actual, "Table should have $colName column")
                assertEquals(expectedSpec, actual, "$colName column spec mismatch")
            }

            val primaryKeys = metadata.getPrimaryKeys(null, null, tableName)
            assertTrue(primaryKeys.next(), "Table should have a primary key")
            assertEquals("user_id", primaryKeys.getString("COLUMN_NAME"), "Primary key should be on user_id")
        }
    }

    @Test
    fun `all three triggers are created successfully`() {
        val result = parser.generate(query)

        connect().use { conn ->
            conn.createStatement().execute(result.summaryTable)

            result.triggers["insert"]?.let { conn.createStatement().execute(it) }
            result.triggers["update"]?.let { conn.createStatement().execute(it) }
            result.triggers["delete"]?.let { conn.createStatement().execute(it) }

            val triggerQuery = """
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
                "Should have an INSERT trigger"
            )

            assertTrue(
                triggers.any { it.key.contains("update", ignoreCase = true) && it.value == "UPDATE" },
                "Should have an UPDATE trigger"
            )

            assertTrue(
                triggers.any { it.key.contains("delete", ignoreCase = true) && it.value == "DELETE" },
                "Should have a DELETE trigger"
            )
        }
    }

    @Test
    fun `original table and summary table match after a single insert`() {
        val result = parser.generate(query)

        connect().use { conn ->
            conn.createStatement().execute("DELETE FROM transactions")

            conn.createStatement().execute(result.summaryTable)
            result.triggers["insert"]?.let { conn.createStatement().execute(it) }
            result.triggers["update"]?.let { conn.createStatement().execute(it) }
            result.triggers["delete"]?.let { conn.createStatement().execute(it) }

            Transactions().seed(1)

            val originalTableQuery = conn.createStatement()
                .executeQuery(query)

            val summaryTableQuery = conn.createStatement()
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