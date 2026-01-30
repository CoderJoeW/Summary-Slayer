package com.coderjoe

import com.coderjoe.database.TransactionService
import com.coderjoe.database.TransactionType
import com.coderjoe.database.TransactionsTable
import com.coderjoe.database.seeders.TransactionsSeeder
import com.coderjoe.services.SummaryTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class SumCostByUserQueryTest : DockerComposeTestBase() {
    private val parser = SummaryTriggerGeneratorSqlParser()
    private val query = queries["sumCostByUser"]!!
    private val summaryTableName = "transactions_user_id_summary"

    private fun setupTriggersAndSummaryTable() {
        val result = parser.generate(query)
        transaction {
            TransactionsTable.deleteAll()
            exec(result.summaryTable)
            result.triggers.values.forEach { exec(it) }
        }
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

    private fun assertTablesMatch(context: String = "") {
        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Summary table should match original query $context".trim())
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

    // --- Single insert scenarios ---

    @Test
    fun `tables match after a single insert`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 25.00)
        assertTablesMatch("after single insert")
    }

    @Test
    fun `tables match after inserting for a new user`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        assertTablesMatch("after inserting for two different users")
    }

    @Test
    fun `tables match after inserting zero-cost transaction`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 0.00)
        assertTablesMatch("after zero-cost insert")
    }

    @Test
    fun `tables match after inserting small fractional cost`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 0.01)
        assertTablesMatch("after 0.01 cost insert")
    }

    @Test
    fun `tables match after inserting large cost`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 99999999.99)
        assertTablesMatch("after max decimal value insert")
    }

    // --- Multiple inserts for same user ---

    @Test
    fun `tables match after multiple inserts for the same user`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(1, 30.00)
        assertTablesMatch("after 3 inserts for same user")
    }

    @Test
    fun `tables match after many small inserts accumulating`() {
        setupTriggersAndSummaryTable()
        repeat(100) {
            insertTransaction(1, 0.01)
        }
        assertTablesMatch("after 100 x 0.01 inserts")
    }

    // --- Multiple inserts across different users ---

    @Test
    fun `tables match after interleaved inserts across multiple users`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)
        insertTransaction(1, 5.00)
        insertTransaction(2, 5.00)
        insertTransaction(3, 5.00)
        assertTablesMatch("after interleaved multi-user inserts")
    }

    @Test
    fun `tables match after bulk inserts across users`() {
        setupTriggersAndSummaryTable()
        val usersAndCounts = mapOf(1 to 50, 2 to 100, 3 to 25)
        usersAndCounts.forEach { (userId, count) ->
            repeat(count) {
                insertTransaction(userId, 2.00)
            }
        }
        assertTablesMatch("after bulk multi-user inserts")
    }

    // --- Single delete scenarios ---

    @Test
    fun `tables match after deleting one transaction from a user with multiple`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(1, 30.00)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting one of three transactions")
    }

    @Test
    fun `tables match after deleting the only transaction for a user`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 50.00)

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting only transaction (user should vanish)")
    }

    @Test
    fun `tables match after deleting all transactions for one user among many`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 2 }
        }
        assertTablesMatch("after removing one user's transactions entirely")
    }

    @Test
    fun `tables match after deleting all transactions from all users`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)

        transaction {
            TransactionsTable.deleteAll()
        }
        assertTablesMatch("after deleting everything (both should be empty)")
    }

    @Test
    fun `tables match after multiple sequential deletes from same user`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(1, 30.00)
        insertTransaction(1, 40.00)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after first delete")

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after second delete")

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after third delete")
    }

    // --- Single update scenarios ---

    @Test
    fun `tables match after updating a transaction cost`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }, limit = 1) {
                it[cost] = 99.00
            }
        }
        assertTablesMatch("after updating one cost")
    }

    @Test
    fun `tables match after updating cost to zero`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 50.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 0.00
            }
        }
        assertTablesMatch("after updating cost to zero")
    }

    @Test
    fun `tables match after updating cost from zero to non-zero`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 0.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 75.50
            }
        }
        assertTablesMatch("after updating from zero to non-zero")
    }

    @Test
    fun `tables match after updating cost to large value`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 1.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 99999999.99
            }
        }
        assertTablesMatch("after updating to max decimal value")
    }

    @Test
    fun `tables match after updating all transactions for a user`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(1, 30.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 5.00
            }
        }
        assertTablesMatch("after updating all of a user's transactions")
    }

    // --- Update user_id (moves cost between groups) ---

    @Test
    fun `tables match after changing user_id on a transaction`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2 WHERE user_id = 1 LIMIT 1",
            )
        }
        assertTablesMatch("after moving a transaction from user 1 to user 2")
    }

    @Test
    fun `tables match after moving all transactions from one user to another`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(2, 5.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2 WHERE user_id = 1",
            )
        }
        assertTablesMatch("after moving all transactions from user 1 to user 2")
    }

    @Test
    fun `tables match after swapping user_ids between transactions`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)

        // Move user 1's transaction to user 3, then user 2's to user 1
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 3 WHERE user_id = 1",
            )
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 1 WHERE user_id = 2",
            )
        }
        assertTablesMatch("after swapping user_ids through intermediate")
    }

    // --- Mixed operations ---

    @Test
    fun `tables match after insert then delete`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after insert then delete")
    }

    @Test
    fun `tables match after insert then update`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 50.00
            }
        }
        assertTablesMatch("after insert then update")
    }

    @Test
    fun `tables match after delete then insert`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting all")

        insertTransaction(1, 99.00)
        assertTablesMatch("after re-inserting for same user")
    }

    @Test
    fun `tables match after full cycle of insert, update, delete across users`() {
        setupTriggersAndSummaryTable()

        // Phase 1: inserts
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)
        assertTablesMatch("after initial inserts")

        // Phase 2: updates
        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 15.00
            }
        }
        assertTablesMatch("after update on user 1")

        // Phase 3: more inserts
        insertTransaction(1, 5.00)
        insertTransaction(2, 10.00)
        assertTablesMatch("after additional inserts")

        // Phase 4: deletes
        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 3 }
        }
        assertTablesMatch("after deleting user 3")

        // Phase 5: insert for deleted user
        insertTransaction(3, 100.00)
        assertTablesMatch("after re-inserting for user 3")
    }

    @Test
    fun `tables match after rapid insert-update-delete sequence on same user`() {
        setupTriggersAndSummaryTable()

        insertTransaction(1, 10.00)
        assertTablesMatch("after insert")

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 50.00
            }
        }
        assertTablesMatch("after update")

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after delete (should be empty)")

        insertTransaction(1, 77.00)
        assertTablesMatch("after re-insert")
    }

    // --- Bulk mixed operations ---

    @Test
    fun `tables match after bulk inserts then bulk deletes`() {
        setupTriggersAndSummaryTable()

        repeat(50) { insertTransaction(1, 1.00) }
        repeat(50) { insertTransaction(2, 2.00) }
        assertTablesMatch("after bulk inserts")

        // Delete half of user 1's transactions
        repeat(25) {
            transaction {
                TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
            }
        }
        assertTablesMatch("after deleting half of user 1's rows")
    }

    @Test
    fun `tables match after bulk inserts then bulk updates`() {
        setupTriggersAndSummaryTable()

        repeat(30) { insertTransaction(1, 10.00) }
        repeat(30) { insertTransaction(2, 20.00) }
        assertTablesMatch("after bulk inserts")

        // Update all of user 1's costs
        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 5.00
            }
        }
        assertTablesMatch("after bulk update of user 1")
    }

    // --- Seeder-based tests ---

    @Test
    fun `tables match after seeding many transactions`() {
        setupTriggersAndSummaryTable()
        TransactionsSeeder().seed(100)
        assertTablesMatch("after seeding 100 transactions")
    }

    @Test
    fun `tables match after seeding then deleting some`() {
        setupTriggersAndSummaryTable()
        TransactionsSeeder().seed(50)
        assertTablesMatch("after seeding 50")

        transaction {
            TransactionsTable.deleteWhere(limit = 10) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting 10 of user 1's seeded rows")
    }

    @Test
    fun `tables match after seeding then updating some`() {
        setupTriggersAndSummaryTable()
        TransactionsSeeder().seed(50)
        assertTablesMatch("after seeding 50")

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }, limit = 10) {
                it[cost] = 999.99
            }
        }
        assertTablesMatch("after updating 10 of user 1's seeded rows")
    }

    // --- Edge cases ---

    @Test
    fun `tables match when user has only zero-cost transactions`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 0.00)
        insertTransaction(1, 0.00)
        insertTransaction(1, 0.00)
        assertTablesMatch("with only zero-cost transactions")
    }

    @Test
    fun `tables match with identical costs across all users`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 10.00)
        insertTransaction(3, 10.00)
        assertTablesMatch("with identical costs per user")
    }

    @Test
    fun `tables match after inserting and deleting same row repeatedly`() {
        setupTriggersAndSummaryTable()

        repeat(10) {
            insertTransaction(1, 5.00)
            assertTablesMatch("after insert #${it + 1}")

            transaction {
                TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
            }
            assertTablesMatch("after delete #${it + 1}")
        }
    }

    @Test
    fun `tables match with many users each having one transaction`() {
        setupTriggersAndSummaryTable()
        // Use users 1, 2, 3 (all that exist via FK)
        insertTransaction(1, 11.11)
        insertTransaction(2, 22.22)
        insertTransaction(3, 33.33)
        assertTablesMatch("with one transaction per user")
    }

    @Test
    fun `tables match after update that does not change the value`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 10.00
            }
        }
        assertTablesMatch("after no-op update (same value)")
    }

    // --- Update both cost and user_id simultaneously ---

    @Test
    fun `tables match after updating both cost and user_id in one statement`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2, cost = 50.00 WHERE user_id = 1 LIMIT 1",
            )
        }
        assertTablesMatch("after updating both cost and user_id simultaneously")
    }

    @Test
    fun `tables match after updating both cost and user_id leaving source user empty`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2, cost = 99.00 WHERE user_id = 1",
            )
        }
        assertTablesMatch("after moving all of user 1 to user 2 with new cost")
    }

    // --- Multi-user delete without LIMIT ---

    @Test
    fun `tables match after deleting rows spanning multiple users by cost filter`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 1.00)
        insertTransaction(1, 50.00)
        insertTransaction(2, 2.00)
        insertTransaction(2, 60.00)
        insertTransaction(3, 3.00)
        insertTransaction(3, 70.00)

        // Delete all cheap transactions across all users
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE cost < 5.00",
            )
        }
        assertTablesMatch("after deleting low-cost rows across multiple users")
    }

    @Test
    fun `tables match after deleting rows that removes some users entirely`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 1.00)
        insertTransaction(2, 2.00)
        insertTransaction(3, 100.00)

        // Delete all rows with cost < 10, wiping out users 1 and 2
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE cost < 10.00",
            )
        }
        assertTablesMatch("after deleting rows that removes users 1 and 2 entirely")
    }

    // --- Update user_id to a brand-new group key ---

    @Test
    fun `tables match after moving transaction to a user with no prior summary entry`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)

        // Move one row to user 2 who has never appeared in the summary table
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2 WHERE user_id = 1 LIMIT 1",
            )
        }
        assertTablesMatch("after moving transaction to user with no prior summary row")
    }

    @Test
    fun `tables match after moving all transactions to a user with no prior summary entry`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)

        // Move everything to user 3 who has never appeared
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 3 WHERE user_id = 1",
            )
        }
        assertTablesMatch("after moving all transactions to brand-new user (source vanishes)")
    }

    // --- Multi-row update changing user_id in one statement ---

    @Test
    fun `tables match after multi-row update changing user_id across groups`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(2, 30.00)

        // Move all of user 1's rows to user 3 in one statement
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 3 WHERE user_id = 1",
            )
        }
        assertTablesMatch("after multi-row user_id change in one statement")
    }

    @Test
    fun `tables match after multi-row update consolidating all users into one`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)

        // Move everyone to user 1
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 1 WHERE user_id != 1",
            )
        }
        assertTablesMatch("after consolidating all transactions to user 1")
    }

    // --- Delete zero-cost transactions ---

    @Test
    fun `tables match after deleting a zero-cost transaction`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 0.00)
        insertTransaction(1, 25.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND cost = 0.00 LIMIT 1",
            )
        }
        assertTablesMatch("after deleting a zero-cost row")
    }

    @Test
    fun `tables match after deleting only zero-cost transaction for a user`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 0.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND cost = 0.00",
            )
        }
        assertTablesMatch("after deleting the only transaction which was zero-cost (user should vanish)")
    }

    @Test
    fun `tables match after deleting all zero-cost transactions across users`() {
        setupTriggersAndSummaryTable()
        insertTransaction(1, 0.00)
        insertTransaction(1, 10.00)
        insertTransaction(2, 0.00)
        insertTransaction(3, 0.00)
        insertTransaction(3, 5.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE cost = 0.00",
            )
        }
        assertTablesMatch("after deleting all zero-cost rows (user 2 should vanish)")
    }
}
