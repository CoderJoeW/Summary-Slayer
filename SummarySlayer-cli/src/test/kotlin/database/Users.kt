package com.coderjoe.database

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

object UsersTable : Table("users") {
    val id = integer("id").autoIncrement()
    val firstName = varchar("first_name", 255)
    val lastName = varchar("last_name", 255)
    override val primaryKey = PrimaryKey(id)
}

class UsersRepository {
    fun insert(
        firstName: String,
        lastName: String,
    ) {
        transaction {
            UsersTable.insert {
                it[UsersTable.firstName] = firstName
                it[UsersTable.lastName] = lastName
            }
        }
    }
}
