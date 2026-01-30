package com.coderjoe.database

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

enum class TransactionType {
    CREDIT,
    DEBIT,
}

enum class TransactionService {
    CALL,
    SMS,
    DATA,
}

object TransactionsTable : Table("transactions") {
    val id = integer("id").autoIncrement()
    val userId = integer("user_id")
    val type = varchar("type", 50)
    val service = varchar("service", 50)
    val cost = double("cost")
    override val primaryKey = PrimaryKey(id)
}

class TransactionsRepository {
    fun insert(
        userId: Int,
        type: TransactionType,
        service: TransactionService,
        cost: Double,
    ) {
        transaction {
            TransactionsTable.insert {
                it[TransactionsTable.userId] = userId
                it[TransactionsTable.type] = type.name
                it[TransactionsTable.service] = service.name
                it[TransactionsTable.cost] = cost
            }
        }
    }

    fun delete(userId: Int, limit: Int? = null) {
        transaction {
            TransactionsTable.deleteWhere(limit = limit) { TransactionsTable.userId eq userId }
        }
    }

    fun lock(lockType: LockType = LockType.WRITE) {
        transaction {
            exec("LOCK TABLES transactions ${lockType.name}")
        }
    }

    fun unlock() {
        transaction {
            exec("UNLOCK TABLES")
        }
    }

    enum class LockType {
        READ,
        WRITE,
    }
}
