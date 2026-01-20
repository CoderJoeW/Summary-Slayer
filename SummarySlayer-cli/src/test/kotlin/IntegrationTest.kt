package com.coderjoe

import com.coderjoe.DockerComposeTestBase.Companion.getJdbcUrl
import com.coderjoe.DockerComposeTestBase.Companion.getPassword
import com.coderjoe.DockerComposeTestBase.Companion.getUsername
import com.coderjoe.services.SummaryTriggerGeneratorSqlParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.sql.DriverManager
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
        DriverManager.getConnection(
            getJdbcUrl(),
            getUsername(),
            getPassword()
        ).use { conn ->
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

        DriverManager.getConnection(
            getJdbcUrl(),
            getUsername(),
            getPassword()
        ).use { conn ->
            // Create the summary table
            conn.createStatement().execute(result.summaryTable)

            // Verify the table was created
            val metadata = conn.metaData
            val tables = metadata.getTables(null, null, "%summary%", arrayOf("TABLE"))
            assertTrue(tables.next(), "Summary table should be created")
            val tableName = tables.getString("TABLE_NAME")

            // Verify table has expected columns with complete specifications
            val columns = metadata.getColumns(null, null, tableName, null)

            data class ColumnSpec(
                val typeName: String,
                val size: Int,
                val decimalDigits: Int?,
                val nullable: Boolean
            )

            val columnSpecs = mutableMapOf<String, ColumnSpec>()
            while (columns.next()) {
                val colName = columns.getString("COLUMN_NAME")
                val typeName = columns.getString("TYPE_NAME")
                val size = columns.getInt("COLUMN_SIZE")
                val decimalDigits = columns.getInt("DECIMAL_DIGITS").takeIf { !columns.wasNull() }
                val nullable = columns.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable

                columnSpecs[colName] = ColumnSpec(typeName, size, decimalDigits, nullable)
            }

            // Expected structure based on query:
            // SELECT user_id, SUM(cost) as total_cost FROM transactions GROUP BY user_id
            // user_id should match the transactions.user_id column: INT NOT NULL
            assertTrue(columnSpecs.containsKey("user_id"), "Table should have user_id column")
            assertEquals("INT", columnSpecs["user_id"]?.typeName, "user_id should be INT type")
            assertEquals(10, columnSpecs["user_id"]?.size, "user_id should have size 10 (INT default)")
            assertEquals(false, columnSpecs["user_id"]?.nullable, "user_id should be NOT NULL (primary key)")

            // total_cost should be DECIMAL(38,6) NOT NULL DEFAULT 0 for SUM aggregate
            assertTrue(columnSpecs.containsKey("total_cost"), "Table should have total_cost column")
            assertEquals("DECIMAL", columnSpecs["total_cost"]?.typeName, "total_cost should be DECIMAL type for SUM")
            assertEquals(38, columnSpecs["total_cost"]?.size, "total_cost should have precision 38")
            assertEquals(6, columnSpecs["total_cost"]?.decimalDigits, "total_cost should have scale 6")
            assertEquals(false, columnSpecs["total_cost"]?.nullable, "total_cost should be NOT NULL")

            // Verify primary key is on user_id
            val primaryKeys = metadata.getPrimaryKeys(null, null, tableName)
            assertTrue(primaryKeys.next(), "Table should have a primary key")
            assertEquals("user_id", primaryKeys.getString("COLUMN_NAME"), "Primary key should be on user_id")
        }
    }
}