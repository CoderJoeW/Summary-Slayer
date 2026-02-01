package com.coderjoe

import com.coderjoe.benchmark.BenchmarkUI
import com.coderjoe.benchmark.DatabaseSeeder
import com.coderjoe.benchmark.QueryStats
import com.coderjoe.benchmark.snapshot
import com.coderjoe.database.DatabaseConfig
import com.coderjoe.services.BackfillService
import com.coderjoe.services.LightningTableTriggerGeneratorSqlParser
import com.coderjoe.services.TriggerGeneratorResult
import com.googlecode.lanterna.input.KeyType
import com.googlecode.lanterna.screen.Screen
import com.googlecode.lanterna.screen.TerminalScreen
import com.googlecode.lanterna.terminal.DefaultTerminalFactory
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.sql.Connection
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class Demo {

    private val parser = LightningTableTriggerGeneratorSqlParser()
    private val seeder = DatabaseSeeder()
    private val ui = BenchmarkUI()

    fun run() {
        val (host, port, username, password, dbName) = promptDatabaseConfig()
        DatabaseConfig.initialize(
            url = "jdbc:mariadb://$host:$port/$dbName",
            username = username,
            password = password,
        )

        val originalQuery = promptQuery()
        val result = parser.generate(originalQuery)

        printSetupPhase(result)

        cleanupPreviousRun(result)
        createLightningTable(result)
        println("  * Lightning Table created")

        println("  o Running backfill (creates triggers + populates)...")
        val backfillStartTime = System.currentTimeMillis()
        BackfillService().backfill(
            result.backfillContext,
            result.triggers.values.toList(),
        ) { completed, total ->
            val barWidth = 40
            val filled = (completed.toDouble() / total * barWidth).toInt()
            val bar = "█".repeat(filled) + "░".repeat(barWidth - filled)
            val pct = (completed.toDouble() / total * 100).toInt()

            val elapsedMs = System.currentTimeMillis() - backfillStartTime
            val elapsedSec = elapsedMs / 1000.0
            val speed = if (elapsedSec > 0) completed / elapsedSec else 0.0

            val progressInfo = if (completed < total && speed > 0) {
                val remainingChunks = total - completed
                val etaSec = (remainingChunks / speed).toInt()
                val elapsedStr = formatTime(elapsedSec.toInt())
                val etaStr = formatTime(etaSec)
                "Elapsed: $elapsedStr | ETA: $etaStr | Speed: ${"%.1f".format(speed)} chunks/sec"
            } else {
                val elapsedStr = formatTime(elapsedSec.toInt())
                "Completed in $elapsedStr"
            }

            print("\r  [$bar] $pct%  ($completed/$total chunks) | $progressInfo")
            if (completed == total) println()
        }
        println("  * Backfill complete - triggers active")

        println()
        println("  Ready to benchmark. Press ENTER to start...")
        readln()

        val lightningQuery = "SELECT * FROM `${result.lightningTableName}`"
        runLiveComparison(originalQuery, lightningQuery, result.lightningTableName)
    }

    private data class DbConfig(
        val host: String,
        val port: String,
        val username: String,
        val password: String,
        val dbName: String,
    )

    private fun promptDatabaseConfig(): DbConfig {
        println()
        println("  LIGHTNING TABLES")
        println("  Lightning Table Generator & Benchmark")
        println()
        println("  Database Connection")
        println("  " + "-".repeat(40))

        print("  Host     (localhost)     : ")
        val host = readln().ifEmpty { "localhost" }
        print("  Port     (3306)         : ")
        val port = readln().ifEmpty { "3306" }
        print("  Username (root)         : ")
        val username = readln().ifEmpty { "root" }
        print("  Password (****)         : ")
        val password = readln().ifEmpty { "rootpassword" }
        print("  Database (summaryslayer): ")
        val dbName = readln().ifEmpty { "summaryslayer" }

        println()
        return DbConfig(host, port, username, password, dbName)
    }

    private fun promptQuery(): String {
        println("  Enter SQL Query (blank for default)")
        println("  " + "-".repeat(40))
        print("  > ")
        val input = readln().trim()

        return if (input.isNotEmpty()) {
            println()
            input
        } else {
            val defaultQuery =
                """
                SELECT user_id, SUM(cost) as total_cost
                FROM transactions
                GROUP BY user_id
                """.trimIndent()
            println("  Using default: ${defaultQuery.replace("\n", " ")}")
            println()
            defaultQuery
        }
    }

    private fun printSetupPhase(result: TriggerGeneratorResult) {
        println("  " + "-".repeat(40))
        println("  Setup Phase")
        println("  " + "-".repeat(40))
        println()
        println("  Lightning Table: ${result.lightningTableName}")
        println("  Triggers:      INSERT / UPDATE / DELETE")
        println()
    }

    private fun runLiveComparison(
        originalQuery: String,
        lightningQuery: String,
        lightningTableName: String,
    ) {
        val running = AtomicBoolean(true)
        val leftStats = QueryStats()
        val rightStats = QueryStats()
        val recordsSeeded = AtomicLong(0)
        val seederError = AtomicReference<String>(null)
        val startTimeNanos = System.nanoTime()

        val terminal = DefaultTerminalFactory().createTerminal()
        val screen: Screen = TerminalScreen(terminal)
        screen.startScreen()
        screen.cursorPosition = null

        val queryExecutor = Executors.newFixedThreadPool(2)
        val seedExecutor = Executors.newSingleThreadExecutor()

        val leftFuture = queryExecutor.submit {
            while (running.get()) {
                executeAndRecord(originalQuery, leftStats)
                Thread.sleep(200)
            }
        }

        val rightFuture = queryExecutor.submit {
            while (running.get()) {
                executeAndRecord(lightningQuery, rightStats)
                Thread.sleep(200)
            }
        }

        try {
            while (running.get()) {
                val key = screen.pollInput()
                if (key != null) {
                    when {
                        key.keyType == KeyType.Enter ||
                            key.keyType == KeyType.Escape ||
                            key.character == 'q' -> break

                        key.character == 's' || key.character == 'S' -> {
                            seedExecutor.submit {
                                try {
                                    seeder.seed(10)
                                    recordsSeeded.addAndGet(10)
                                } catch (e: Exception) {
                                    seederError.set("${e.javaClass.simpleName}: ${e.message}")
                                }
                            }
                        }
                    }
                }

                val size = screen.terminalSize
                val tg = screen.newTextGraphics()
                screen.clear()

                val elapsedSecs = (System.nanoTime() - startTimeNanos) / 1_000_000_000.0

                ui.drawDashboard(
                    tg,
                    size,
                    snapshot(leftStats),
                    snapshot(rightStats),
                    originalQuery,
                    lightningQuery,
                    elapsedSecs,
                    recordsSeeded.get(),
                    seederError.get(),
                )

                screen.refresh()
                Thread.sleep(250)
            }
        } finally {
            running.set(false)
            leftFuture.cancel(true)
            rightFuture.cancel(true)
            queryExecutor.shutdownNow()
            seedExecutor.shutdownNow()

            screen.stopScreen()
        }
    }

    private fun executeAndRecord(query: String, stats: QueryStats) {
        try {
            transaction {
                val conn = this.connection.connection as Connection
                conn.createStatement().use { stmt ->
                    val start = System.currentTimeMillis()
                    val rs = stmt.executeQuery(query)
                    val end = System.currentTimeMillis()
                    val elapsed = end - start

                    synchronized(stats) {
                        stats.totalRuns++
                        stats.totalTimeMs += elapsed
                        stats.lastTimeMs = elapsed
                        if (elapsed < stats.minTimeMs) stats.minTimeMs = elapsed
                        if (elapsed > stats.maxTimeMs) stats.maxTimeMs = elapsed

                        val rows = mutableListOf<List<String>>()
                        val columnNames = mutableListOf<String>()
                        val meta = rs.metaData
                        for (i in 1..meta.columnCount) {
                            columnNames.add(meta.getColumnLabel(i))
                        }
                        while (rs.next()) {
                            val row = mutableListOf<String>()
                            for (i in 1..meta.columnCount) {
                                row.add(rs.getString(i) ?: "NULL")
                            }
                            rows.add(row)
                        }
                        stats.columnNames = columnNames
                        stats.lastRows = rows
                    }
                }
            }
        } catch (_: Exception) {
            // swallow during shutdown
        }
    }

    private fun cleanupPreviousRun(result: TriggerGeneratorResult) {
        val base = result.backfillContext.baseTableName
        val triggerNames = listOf(
            "${base}_after_insert_lightning",
            "${base}_after_update_lightning",
            "${base}_after_delete_lightning",
        )
        transaction {
            val conn = this.connection.connection as Connection
            triggerNames.forEach { name ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP TRIGGER IF EXISTS `$name`")
                }
            }
            conn.createStatement().use { stmt ->
                stmt.execute("DROP TABLE IF EXISTS `${result.lightningTableName}`")
            }
        }
    }

    private fun createLightningTable(result: TriggerGeneratorResult) {
        println("  o Creating Lightning Table...")
        transaction {
            exec(result.lightningTable)
        }
    }

    private fun formatTime(seconds: Int): String {
        val mins = seconds / 60
        val secs = seconds % 60
        return "%02d:%02d".format(mins, secs)
    }
}
