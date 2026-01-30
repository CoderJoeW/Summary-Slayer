package com.coderjoe.services

import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.sql.Timestamp
import java.util.concurrent.Executors

private const val TIMESTAMP_COLUMN = "updated_at"

class BackfillService(
    private val chunkSize: Int = 5_000,
    private val threadCount: Int = 4
) {

    fun backfill(context: BackfillContext, triggerStatements: List<String>) {
        val primaryKeyColumn = detectPrimaryKey(context.baseTableName)
        validateUpdatedAtColumn(context.baseTableName)

        val snapshot = lockCreateTriggersAndCaptureSnapshot(context, primaryKeyColumn, triggerStatements)
        if (snapshot == null) {
            println("No rows to backfill.")
            return
        }

        val sql = buildBackfillSql(context, primaryKeyColumn)
        processChunks(snapshot, sql)
    }

    private data class BackfillSnapshot(
        val dbNow: Timestamp,
        val minPk: Long,
        val maxPk: Long
    )

    private fun lockCreateTriggersAndCaptureSnapshot(
        context: BackfillContext,
        primaryKeyColumn: String,
        triggerStatements: List<String>
    ): BackfillSnapshot? {
        return transaction {
            val conn = this.connection.connection as java.sql.Connection

            conn.createStatement().execute(
                "LOCK TABLES `${context.baseTableName}` WRITE, `${context.summaryTableName}` WRITE"
            )

            try {
                triggerStatements.forEach { sql ->
                    conn.createStatement().use { stmt -> stmt.execute(sql) }
                }

                val dbNow = conn.createStatement().use { stmt ->
                    val rs = stmt.executeQuery("SELECT NOW() AS now_ts")
                    rs.next()
                    rs.getTimestamp("now_ts")
                }

                val (minPk, maxPk) = conn.prepareStatement(
                    "SELECT MIN(`$primaryKeyColumn`) AS min_pk, MAX(`$primaryKeyColumn`) AS max_pk FROM `${context.baseTableName}` WHERE `$TIMESTAMP_COLUMN` <= ?"
                ).use { stmt ->
                    stmt.setTimestamp(1, dbNow)
                    val rs = stmt.executeQuery()
                    if (rs.next()) {
                        val min = rs.getLong("min_pk").takeIf { !rs.wasNull() }
                        val max = rs.getLong("max_pk").takeIf { !rs.wasNull() }
                        Pair(min, max)
                    } else {
                        Pair(null, null)
                    }
                }

                conn.createStatement().execute("TRUNCATE TABLE `${context.summaryTableName}`")

                if (minPk == null || maxPk == null) null
                else BackfillSnapshot(dbNow, minPk, maxPk)
            } finally {
                conn.createStatement().execute("UNLOCK TABLES")
            }
        }
    }

    private fun validateUpdatedAtColumn(baseTableName: String) {
        transaction {
            val conn = this.connection.connection as java.sql.Connection
            val databaseName = conn.catalog

            val sql = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'updated_at'
            """.trimIndent()

            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, databaseName)
                stmt.setString(2, baseTableName)

                val rs = stmt.executeQuery()
                if (!rs.next()) {
                    throw IllegalStateException(
                        "Required column `updated_at` not found on `$baseTableName`. " +
                            "The base table must have an `updated_at` timestamp column for backfill."
                    )
                }
            }
        }
    }

    private fun detectPrimaryKey(baseTableName: String): String {
        return transaction {
            val conn = this.connection.connection as java.sql.Connection
            val databaseName = conn.catalog

            val sql = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'
            """.trimIndent()

            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, databaseName)
                stmt.setString(2, baseTableName)

                val rs = stmt.executeQuery()
                val pkColumns = mutableListOf<String>()
                while (rs.next()) {
                    pkColumns.add(rs.getString("COLUMN_NAME"))
                }

                if (pkColumns.size != 1) {
                    throw IllegalStateException("Expected a single numeric primary key on `$baseTableName`, found ${pkColumns.size}.")
                }

                pkColumns.first()
            }
        }
    }

    private fun buildBackfillSql(context: BackfillContext, primaryKeyColumn: String): String {
        val groupCols = context.groupByColumns.joinToString(", ") { "`$it`" }
        val aggSelectCols = context.aggregates.joinToString(", ") { agg ->
            if (agg.func == "COUNT") {
                "COUNT(*) AS `${agg.alias}`"
            } else {
                "${agg.func}(`${agg.col}`) AS `${agg.alias}`"
            }
        }
        val aggInsertCols = context.aggregates.joinToString(", ") { "`${it.alias}`" }

        val insertCols = if (groupCols.isNotEmpty()) "$groupCols, $aggInsertCols" else aggInsertCols
        val selectCols = if (groupCols.isNotEmpty()) "$groupCols, $aggSelectCols" else aggSelectCols

        val whereParts = mutableListOf<String>()
        context.whereClause?.let { whereParts.add("($it)") }
        whereParts.add("`$primaryKeyColumn` BETWEEN ? AND ?")
        whereParts.add("`$TIMESTAMP_COLUMN` <= ?")

        val whereStr = whereParts.joinToString(" AND ")

        val groupByStr = if (context.groupByColumns.isNotEmpty()) {
            "GROUP BY $groupCols"
        } else {
            ""
        }

        val onDuplicateUpdate = context.aggregates.joinToString(", ") { agg ->
            "`${agg.alias}` = `${agg.alias}` + VALUES(`${agg.alias}`)"
        }

        return """
            INSERT INTO `${context.summaryTableName}` ($insertCols)
            SELECT $selectCols
            FROM `${context.baseTableName}`
            WHERE $whereStr
            $groupByStr
            ON DUPLICATE KEY UPDATE $onDuplicateUpdate
        """.trimIndent()
    }

    private fun processChunks(snapshot: BackfillSnapshot, sql: String) {
        val executor = Executors.newFixedThreadPool(threadCount)
        val chunks = mutableListOf<Pair<Long, Long>>()

        var current = snapshot.minPk
        while (current <= snapshot.maxPk) {
            val end = minOf(current + chunkSize - 1, snapshot.maxPk)
            chunks.add(Pair(current, end))
            current = end + 1
        }

        val totalChunks = chunks.size
        println("Processing $totalChunks chunks (PK range ${snapshot.minPk}..${snapshot.maxPk}, chunk size $chunkSize)")

        val futures = chunks.mapIndexed { index, (start, end) ->
            executor.submit<Unit> {
                transaction {
                    val conn = this.connection.connection as java.sql.Connection
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setLong(1, start)
                        stmt.setLong(2, end)
                        stmt.setTimestamp(3, snapshot.dbNow)
                        stmt.executeUpdate()
                    }
                }
                println("  Chunk ${index + 1}/$totalChunks done (PK $start..$end)")
            }
        }

        futures.forEach { it.get() }
        executor.shutdown()
    }
}
