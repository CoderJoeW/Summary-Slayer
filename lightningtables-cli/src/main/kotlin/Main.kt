package com.coderjoe

import com.coderjoe.services.LightningTableTriggerGeneratorSqlParser
import com.coderjoe.services.TriggerGeneratorResult
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

fun main() {
    Demo().run()
}

fun seed(recordCount: Int) {
    val batchSize = 5_000
    val threadCount = 4
    val startTime = System.currentTimeMillis()
    val insertedCount = AtomicInteger(0)

    val executor = Executors.newFixedThreadPool(threadCount)
    val futures = (0 until recordCount).chunked(batchSize).map { batch ->
        executor.submit {
            transaction {
                TransactionsTable.batchInsert(batch) {
                    this[TransactionsTable.userId] = Random.nextInt(1, 5)
                    this[TransactionsTable.type] = TransactionType.DEBIT.name
                    this[TransactionsTable.service] = TransactionService.entries.random().name
                    this[TransactionsTable.cost] = Random.nextDouble(0.01, 2.0)
                }
            }

            val total = insertedCount.addAndGet(batch.size)
            if (total % 100_000 < batchSize) {
                println("Inserted $total records...")
            }
        }
    }

    futures.forEach { it.get() }
    executor.shutdown()

    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 1000.0

    println("Completed! Inserted $recordCount records in $duration seconds")
}

object TransactionsTable : Table("transactions") {
    val id = integer("id").autoIncrement()
    val userId = integer("user_id")
    val type = varchar("type", 50)
    val service = varchar("service", 50)
    val cost = double("cost")
    override val primaryKey = PrimaryKey(id)
}

enum class TransactionType {
    CREDIT,
    DEBIT,
}

enum class TransactionService {
    CALL,
    SMS,
    DATA,
}

fun testLightningTableGen() {
    val parser = LightningTableTriggerGeneratorSqlParser()

    val query =
        """
        SELECT user_id, SUM(cost) as total_cost
        FROM transactions
        GROUP BY user_id
        """.trimIndent()

    try {
        val result = parser.generate(query)

        println("=== Generated Lightning Table DDL ===")
        println(result.lightningTable)
        println()

        println("=== Generated Triggers ===")
        println("\n-- INSERT Trigger:")
        println(result.triggers["insert"])
        println("\n-- UPDATE Trigger:")
        println(result.triggers["update"])
        println("\n-- DELETE Trigger:")
        println(result.triggers["delete"])
        println()

        println("=== Complete Preview ===")
        println(result.preview)
    } catch (e: Exception) {
        println("Error generating triggers: ${e.message}")
        e.printStackTrace()
    }
}
