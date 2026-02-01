package com.coderjoe

import com.coderjoe.database.DatabaseConfig
import com.coderjoe.services.BackfillService
import com.coderjoe.services.LightningTableTriggerGeneratorSqlParser
import com.coderjoe.services.TriggerGeneratorResult
import com.googlecode.lanterna.SGR
import com.googlecode.lanterna.TerminalSize
import com.googlecode.lanterna.TextColor
import com.googlecode.lanterna.graphics.TextGraphics
import com.googlecode.lanterna.input.KeyType
import com.googlecode.lanterna.screen.Screen
import com.googlecode.lanterna.screen.TerminalScreen
import com.googlecode.lanterna.terminal.DefaultTerminalFactory
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.io.OutputStream
import java.io.PrintStream
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class Demo {

    private val parser = LightningTableTriggerGeneratorSqlParser()

    fun run() {
        val (host, port, username, password, dbName) =
            promptDatabaseConfig()
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
        BackfillService().backfill(
            result.backfillContext,
            result.triggers.values.toList(),
        ) { completed, total ->
            val barWidth = 40
            val filled = (completed.toDouble() / total * barWidth).toInt()
            val bar = "█".repeat(filled) + "░".repeat(barWidth - filled)
            val pct = (completed.toDouble() / total * 100).toInt()
            print("\r  [$bar] $pct%  ($completed/$total chunks)")
            if (completed == total) println()
        }
        println("  * Backfill complete - triggers active")

        println()
        println("  Ready to benchmark. Press ENTER to start...")
        readln()

        val lightningQuery =
            "SELECT * FROM `${result.lightningTableName}`"
        runLiveComparison(
            originalQuery,
            lightningQuery,
            result.lightningTableName,
        )
    }

    // -- Setup prompts ---------------------------------------------------

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
        val input = readln()
        println()

        return input.ifEmpty {
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

    // -- Live comparison -------------------------------------------------

    private data class QueryStats(
        var totalRuns: Int = 0,
        var totalTimeMs: Long = 0,
        var lastTimeMs: Long = 0,
        var minTimeMs: Long = Long.MAX_VALUE,
        var maxTimeMs: Long = 0,
        var lastRows: List<List<String>> = emptyList(),
        var columnNames: List<String> = emptyList(),
    )

    private data class StatsSnapshot(
        val totalRuns: Int = 0,
        val lastTimeMs: Long = 0,
        val avgTimeMs: Double = 0.0,
        val minTimeMs: Long = Long.MAX_VALUE,
        val maxTimeMs: Long = 0,
        val rowCount: Int = 0,
        val columnNames: List<String> = emptyList(),
        val rows: List<List<String>> = emptyList(),
    )

    private fun snapshot(s: QueryStats): StatsSnapshot =
        synchronized(s) {
            StatsSnapshot(
                totalRuns = s.totalRuns,
                lastTimeMs = s.lastTimeMs,
                avgTimeMs =
                    if (s.totalRuns > 0) {
                        s.totalTimeMs.toDouble() / s.totalRuns
                    } else {
                        0.0
                    },
                minTimeMs = s.minTimeMs,
                maxTimeMs = s.maxTimeMs,
                rowCount = s.lastRows.size,
                columnNames = s.columnNames,
                rows = s.lastRows,
            )
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

        // Create a Lanterna screen (enters private/alt mode automatically)
        val terminal = DefaultTerminalFactory().createTerminal()
        val screen: Screen = TerminalScreen(terminal)
        screen.startScreen()
        screen.cursorPosition = null // hide cursor

        val executor = Executors.newFixedThreadPool(3)

        val leftFuture = executor.submit {
            while (running.get()) {
                executeAndRecord(originalQuery, leftStats)
                Thread.sleep(200)
            }
        }

        val rightFuture = executor.submit {
            while (running.get()) {
                executeAndRecord(lightningQuery, rightStats)
                Thread.sleep(200)
            }
        }

        val seederFuture = executor.submit {
            val batchSize = 10
            while (running.get()) {
                try {
                    seed(batchSize)
                    recordsSeeded.addAndGet(batchSize.toLong())
                } catch (e: Exception) {
                    seederError.set(
                        "${e.javaClass.simpleName}: ${e.message}",
                    )
                }
                try {
                    Thread.sleep(60_000)
                } catch (_: InterruptedException) {
                    break
                }
            }
        }

        try {
            while (running.get()) {
                // Check for keypress (non-blocking)
                val key = screen.pollInput()
                if (key != null &&
                    (key.keyType == KeyType.Enter ||
                        key.keyType == KeyType.Escape ||
                        key.character == 'q')
                ) {
                    break
                }

                val size = screen.terminalSize
                val tg = screen.newTextGraphics()

                // Clear the back buffer
                screen.clear()

                val elapsedSecs =
                    (System.nanoTime() - startTimeNanos) / 1_000_000_000.0

                drawDashboard(
                    tg,
                    size,
                    snapshot(leftStats),
                    snapshot(rightStats),
                    originalQuery,
                    lightningQuery,
                    lightningTableName,
                    elapsedSecs,
                    recordsSeeded.get(),
                    seederError.get(),
                )

                // Refresh sends only the diff to the terminal
                screen.refresh()
                Thread.sleep(250)
            }
        } finally {
            running.set(false)
            leftFuture.cancel(true)
            rightFuture.cancel(true)
            seederFuture.cancel(true)
            executor.shutdownNow()

            val totalElapsedSecs =
                (System.nanoTime() - startTimeNanos) / 1_000_000_000.0

            screen.stopScreen()

            renderFinalReport(
                snapshot(leftStats),
                snapshot(rightStats),
                lightningTableName,
                totalElapsedSecs,
                recordsSeeded.get(),
            )
        }
    }

    private fun executeAndRecord(query: String, stats: QueryStats) {
        try {
            transaction {
                val conn =
                    this.connection.connection as java.sql.Connection
                conn.createStatement().use { stmt ->
                    val start = System.nanoTime()
                    val rs = stmt.executeQuery(query)
                    val meta = rs.metaData
                    val colCount = meta.columnCount
                    val colNames =
                        (1..colCount).map { meta.getColumnLabel(it) }
                    val rows = mutableListOf<List<String>>()
                    while (rs.next()) {
                        rows.add(
                            (1..colCount).map {
                                rs.getString(it) ?: "NULL"
                            },
                        )
                    }
                    val elapsed =
                        (System.nanoTime() - start) / 1_000_000

                    synchronized(stats) {
                        stats.totalRuns++
                        stats.totalTimeMs += elapsed
                        stats.lastTimeMs = elapsed
                        stats.minTimeMs =
                            minOf(stats.minTimeMs, elapsed)
                        stats.maxTimeMs =
                            maxOf(stats.maxTimeMs, elapsed)
                        stats.columnNames = colNames
                        stats.lastRows = rows
                    }
                }
            }
        } catch (_: Exception) {
            // swallow during shutdown
        }
    }

    // -- Screen drawing --------------------------------------------------

    private fun drawDashboard(
        tg: TextGraphics,
        size: TerminalSize,
        left: StatsSnapshot,
        right: StatsSnapshot,
        originalQuery: String,
        lightningQuery: String,
        lightningTableName: String,
        elapsedSecs: Double,
        recordsSeeded: Long,
        seederError: String?,
    ) {
        val w = size.columns
        val h = size.rows
        val midCol = w / 2

        // ── Title bar ──────────────────────────────────────────────
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.backgroundColor = TextColor.ANSI.BLUE
        tg.enableModifiers(SGR.BOLD)
        val title = " LIGHTNING TABLES  -  Live Benchmark "
        val elapsed = formatElapsed(elapsedSecs)
        val seeded = "%,d".format(recordsSeeded)
        val rightInfo = "Seeded: $seeded  |  $elapsed "
        val gap = maxOf(0, w - title.length - rightInfo.length)
        tg.putString(0, 0, title + " ".repeat(gap) + rightInfo)
        tg.disableModifiers(SGR.BOLD)
        tg.backgroundColor = TextColor.ANSI.DEFAULT

        if (seederError != null) {
            tg.foregroundColor = TextColor.ANSI.RED
            tg.putString(
                1,
                1,
                "Seeder error: ${truncate(seederError, w - 2)}",
            )
            tg.foregroundColor = TextColor.ANSI.DEFAULT
        }

        // ── Panel headers ──────────────────────────────────────────
        var row = 2

        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.backgroundColor = TextColor.ANSI.MAGENTA
        tg.enableModifiers(SGR.BOLD)
        val lHeader = centerPad("ORIGINAL QUERY", midCol)
        tg.putString(0, row, lHeader)
        tg.backgroundColor = TextColor.ANSI.DEFAULT
        tg.putString(midCol, row, " ")
        tg.backgroundColor = TextColor.ANSI.GREEN
        tg.foregroundColor = TextColor.ANSI.BLACK
        val rHeader = centerPad("LIGHTNING TABLE", w - midCol - 1)
        tg.putString(midCol + 1, row, rHeader)
        tg.backgroundColor = TextColor.ANSI.DEFAULT
        tg.disableModifiers(SGR.BOLD)
        tg.foregroundColor = TextColor.ANSI.DEFAULT

        // ── Divider column ─────────────────────────────────────────
        tg.foregroundColor = TextColor.ANSI.WHITE
        for (r in 3 until h - 1) {
            tg.putString(midCol, r, "|")
        }
        tg.foregroundColor = TextColor.ANSI.DEFAULT

        // ── Stats ──────────────────────────────────────────────────
        row = 4
        row = drawStats(tg, 2, row, left)
        val rightStatsStart = 4
        drawStats(tg, midCol + 2, rightStatsStart, right)

        // ── Speedup indicator ──────────────────────────────────────
        row += 1
        val speedupMsg = speedupMessage(left, right)
        tg.enableModifiers(SGR.BOLD)
        tg.foregroundColor = when {
            left.avgTimeMs > 0 && right.avgTimeMs > 0 &&
                right.avgTimeMs <= left.avgTimeMs ->
                TextColor.ANSI.GREEN
            else -> TextColor.ANSI.YELLOW
        }
        tg.putString(maxOf(0, (w - speedupMsg.length) / 2), row, speedupMsg)
        tg.foregroundColor = TextColor.ANSI.DEFAULT
        tg.disableModifiers(SGR.BOLD)

        // ── Horizontal separator ───────────────────────────────────
        row += 2
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.putString(0, row, "-".repeat(w))
        tg.foregroundColor = TextColor.ANSI.DEFAULT

        // ── Query labels ───────────────────────────────────────────
        row += 1
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.putString(
            2,
            row,
            "Left:  " + truncate(
                originalQuery.replace("\n", " ").trim(),
                midCol - 10,
            ),
        )
        tg.putString(
            midCol + 2,
            row,
            "Right: " + truncate(lightningQuery, w - midCol - 12),
        )
        tg.foregroundColor = TextColor.ANSI.DEFAULT

        // ── Separator ──────────────────────────────────────────────
        row += 1
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.putString(0, row, "-".repeat(w))
        tg.foregroundColor = TextColor.ANSI.DEFAULT

        // ── Data tables ────────────────────────────────────────────
        row += 1
        val maxDataRows = h - row - 2 // reserve 1 for footer
        drawDataTable(tg, 1, row, midCol - 2, left, maxDataRows)
        drawDataTable(
            tg,
            midCol + 2,
            row,
            w - midCol - 3,
            right,
            maxDataRows,
        )

        // ── Footer ─────────────────────────────────────────────────
        tg.foregroundColor = TextColor.ANSI.WHITE
        val footer = "Press ENTER or 'q' to stop"
        tg.putString(maxOf(0, (w - footer.length) / 2), h - 1, footer)
        tg.foregroundColor = TextColor.ANSI.DEFAULT
    }

    private fun drawStats(
        tg: TextGraphics,
        col: Int,
        startRow: Int,
        snap: StatsSnapshot,
    ): Int {
        var r = startRow
        tg.foregroundColor = TextColor.ANSI.DEFAULT
        tg.putString(col, r++, "Executions : ${snap.totalRuns}")
        tg.putString(col, r++, "Last       : ${fmtMs(snap.lastTimeMs)}")
        tg.putString(
            col,
            r++,
            "Avg        : ${"%.2f".format(snap.avgTimeMs)} ms",
        )
        tg.putString(col, r++, "Min        : ${fmtMs(snap.minTimeMs)}")
        tg.putString(col, r++, "Max        : ${fmtMs(snap.maxTimeMs)}")
        tg.putString(col, r++, "Rows       : ${snap.rowCount}")
        return r
    }

    private fun drawDataTable(
        tg: TextGraphics,
        col: Int,
        startRow: Int,
        maxWidth: Int,
        snap: StatsSnapshot,
        maxRows: Int,
    ) {
        if (snap.columnNames.isEmpty()) {
            tg.foregroundColor = TextColor.ANSI.WHITE
            tg.putString(col, startRow, "(waiting...)")
            tg.foregroundColor = TextColor.ANSI.DEFAULT
            return
        }

        val numCols = snap.columnNames.size
        val colWidth = maxOf(1, maxWidth / numCols)
        var r = startRow

        // Header
        tg.enableModifiers(SGR.BOLD)
        tg.foregroundColor = TextColor.ANSI.CYAN
        snap.columnNames.forEachIndexed { i, name ->
            tg.putString(
                col + i * colWidth,
                r,
                fitStr(name, colWidth),
            )
        }
        tg.disableModifiers(SGR.BOLD)
        r++

        // Separator
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.putString(col, r, "-".repeat(minOf(maxWidth, numCols * colWidth)))
        tg.foregroundColor = TextColor.ANSI.DEFAULT
        r++

        // Data rows
        val displayRows = snap.rows.take(maxOf(0, maxRows - 2))
        displayRows.forEach { rowData ->
            rowData.forEachIndexed { i, value ->
                tg.putString(
                    col + i * colWidth,
                    r,
                    fitStr(value, colWidth),
                )
            }
            r++
        }

        if (snap.rows.size > displayRows.size) {
            tg.foregroundColor = TextColor.ANSI.WHITE
            tg.putString(
                col,
                r,
                "... ${snap.rows.size - displayRows.size} more rows",
            )
            tg.foregroundColor = TextColor.ANSI.DEFAULT
        }
    }

    // -- Final report (printed to normal terminal after screen closes) ----

    private fun renderFinalReport(
        left: StatsSnapshot,
        right: StatsSnapshot,
        lightningTableName: String,
        totalElapsedSecs: Double,
        totalRecordsSeeded: Long,
    ) {
        println()
        println("  " + "=".repeat(60))
        println("  Final Report")
        println("  " + "=".repeat(60))
        println()
        println("  Duration       : ${formatElapsed(totalElapsedSecs)}")
        println("  Records seeded : %,d".format(totalRecordsSeeded))
        println()

        println("  Original Query")
        println("    Executions : ${left.totalRuns}")
        println("    Avg time   : ${"%.2f".format(left.avgTimeMs)} ms")
        println("    Min time   : ${fmtMs(left.minTimeMs)}")
        println("    Max time   : ${fmtMs(left.maxTimeMs)}")
        println("    Rows       : ${left.rowCount}")
        println()
        println("  Lightning Table ($lightningTableName)")
        println("    Executions : ${right.totalRuns}")
        println("    Avg time   : ${"%.2f".format(right.avgTimeMs)} ms")
        println("    Min time   : ${fmtMs(right.minTimeMs)}")
        println("    Max time   : ${fmtMs(right.maxTimeMs)}")
        println("    Rows       : ${right.rowCount}")
        println()

        println("  ${speedupMessage(left, right)}")
        println()
        println("  " + "=".repeat(60))
    }

    // -- Helpers ---------------------------------------------------------

    private fun cleanupPreviousRun(
        result: TriggerGeneratorResult,
    ) {
        val base = result.backfillContext.baseTableName
        val triggerNames = listOf(
            "${base}_after_insert_lightning",
            "${base}_after_update_lightning",
            "${base}_after_delete_lightning",
        )
        transaction {
            val conn =
                this.connection.connection as java.sql.Connection
            triggerNames.forEach { name ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        "DROP TRIGGER IF EXISTS `$name`",
                    )
                }
            }
            conn.createStatement().use { stmt ->
                stmt.execute(
                    "DROP TABLE IF EXISTS" +
                        " `${result.lightningTableName}`",
                )
            }
        }
    }

    private fun createLightningTable(
        result: TriggerGeneratorResult,
    ) {
        println("  o Creating Lightning Table...")
        transaction {
            exec(result.lightningTable)
        }
    }

    private inline fun suppressStdout(block: () -> Unit) {
        val realOut = System.out
        val realErr = System.err
        val devNull = PrintStream(NullOutputStream)
        System.setOut(devNull)
        System.setErr(devNull)
        try {
            block()
        } finally {
            System.setOut(realOut)
            System.setErr(realErr)
        }
    }

    private object NullOutputStream : OutputStream() {
        override fun write(b: Int) {}
        override fun write(b: ByteArray, off: Int, len: Int) {}
    }

    private fun speedupMessage(
        left: StatsSnapshot,
        right: StatsSnapshot,
    ): String {
        if (left.avgTimeMs <= 0 || right.avgTimeMs <= 0) {
            return "Collecting data..."
        }
        val ratio = left.avgTimeMs / right.avgTimeMs
        return if (ratio >= 1.0) {
            "Lightning Table is %.1fx faster".format(ratio)
        } else {
            "Lightning Table is %.1fx slower".format(1.0 / ratio)
        }
    }

    private fun formatElapsed(secs: Double): String {
        val totalSecs = secs.toLong()
        val h = totalSecs / 3600
        val m = (totalSecs % 3600) / 60
        val s = totalSecs % 60
        return "%02d:%02d:%02d".format(h, m, s)
    }

    private fun fmtMs(ms: Long): String =
        if (ms == Long.MAX_VALUE) "-" else "$ms ms"

    private fun truncate(s: String, maxLen: Int): String =
        if (s.length > maxLen) s.take(maxLen - 3) + "..." else s

    private fun fitStr(s: String, width: Int): String =
        if (s.length >= width) s.take(width - 1) + " " else s.padEnd(width)

    private fun centerPad(text: String, width: Int): String {
        val pad = maxOf(0, width - text.length)
        val l = pad / 2
        val r = pad - l
        return " ".repeat(l) + text + " ".repeat(r)
    }
}
