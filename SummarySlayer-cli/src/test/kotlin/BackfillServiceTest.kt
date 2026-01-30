package com.coderjoe

import com.coderjoe.database.TransactionService
import com.coderjoe.database.TransactionType
import com.coderjoe.database.TransactionsTable
import com.coderjoe.services.BackfillService
import com.coderjoe.services.SummaryTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class BackfillServiceTest : DockerComposeTestBase() {
    private val parser = SummaryTriggerGeneratorSqlParser()
    private val query = queries["sumCostByUser"]!!
    private val summaryTableName = "transactions_user_id_summary"

    private fun setupSummaryTableAndBackfill(chunkSize: Int = 10) {
        val result = parser.generate(query)
        transaction {
            exec(result.summaryTable)
        }
        BackfillService(chunkSize = chunkSize, threadCount = 2)
            .backfill(result.backfillContext, result.triggers.values.toList())
    }

    private fun queryOriginalTable(): Map<Int, BigDecimal> {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery(query)
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            return results
        }
    }

    private fun querySummaryTable(): Map<Int, BigDecimal> {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM $summaryTableName")
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            return results
        }
    }

    private fun summaryRowCount(): Int {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT COUNT(*) AS cnt FROM $summaryTableName")
            rs.next()
            return rs.getInt("cnt")
        }
    }

    private fun insertTransaction(userId: Int, cost: Double) {
        transaction {
            TransactionsTable.insert {
                it[TransactionsTable.userId] = userId
                it[TransactionsTable.type] = TransactionType.DEBIT.name
                it[TransactionsTable.service] = TransactionService.CALL.name
                it[TransactionsTable.cost] = cost
            }
        }
    }

    @Test
    fun `backfill matches original query`() {
        setupSummaryTableAndBackfill()

        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Backfilled summary should match original query")
    }

    @Test
    fun `backfill on empty table produces empty summary`() {
        connect().use { conn ->
            conn.createStatement().execute("DELETE FROM transactions")
        }

        setupSummaryTableAndBackfill()

        assertEquals(0, summaryRowCount(), "Summary table should be empty when base table is empty")
    }

    @Test
    fun `backfill plus new inserts match original query`() {
        setupSummaryTableAndBackfill()

        // Insert additional rows — triggers handle these
        insertTransaction(1, 100.00)
        insertTransaction(2, 200.00)
        insertTransaction(3, 300.00)

        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Summary should match after backfill + trigger inserts")
    }

    @Test
    fun `backfill with small chunk size covers all rows`() {
        setupSummaryTableAndBackfill(chunkSize = 2)

        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Multi-chunk backfill should match original query")
    }

    @Test
    fun `backfill with large chunk size works`() {
        setupSummaryTableAndBackfill(chunkSize = 100_000)

        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Single-chunk backfill should match original query")
    }

    @Test
    fun `backfill is idempotent via truncate`() {
        // First run creates triggers + backfills
        setupSummaryTableAndBackfill()

        // Second run: triggers already exist, so just re-backfill
        val result = parser.generate(query)
        BackfillService(chunkSize = 10, threadCount = 2)
            .backfill(result.backfillContext, emptyList())

        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Second backfill should produce same result")
    }

    @Test
    fun `backfill handles many rows with multiple chunks`() {
        for (i in 1..50) {
            insertTransaction(1, 1.00)
        }

        setupSummaryTableAndBackfill(chunkSize = 10)

        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Backfill with many rows should match")
    }

    @Test
    fun `backfill summary has correct number of groups`() {
        setupSummaryTableAndBackfill()

        val originalGroups = queryOriginalTable().size
        val summaryGroups = summaryRowCount()
        assertEquals(originalGroups, summaryGroups, "Summary should have same number of groups as original")
        assertTrue(summaryGroups > 0, "Should have at least one group")
    }
}
