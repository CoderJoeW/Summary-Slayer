package com.coderjoe.database.seeders

import com.coderjoe.database.TransactionService
import com.coderjoe.database.TransactionType
import com.coderjoe.database.TransactionsRepository
import kotlin.random.Random
import kotlin.text.insert

class TransactionsSeeder {
    fun seed(recordCount: Int) {
        val repository = TransactionsRepository()

        val startTime = System.currentTimeMillis()

        repeat(recordCount) { index ->
            val cost = Random.nextDouble(0.01, 2.0)
            val service = TransactionService.entries.random()
            repository.insert(1, TransactionType.DEBIT, service, cost)

            if ((index + 1) % 10_000 == 0) {
                println("Inserted ${index + 1} records...")
            }
        }

        val endTime = System.currentTimeMillis()
        val duration = (endTime - startTime) / 1000.0

        println("Completed! Inserted $recordCount records in $duration seconds")
    }
}
