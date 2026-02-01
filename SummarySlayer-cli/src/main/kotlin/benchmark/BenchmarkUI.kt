package com.coderjoe.benchmark

import com.googlecode.lanterna.SGR
import com.googlecode.lanterna.TerminalSize
import com.googlecode.lanterna.TextColor
import com.googlecode.lanterna.graphics.TextGraphics

class BenchmarkUI {

    fun drawDashboard(
        tg: TextGraphics,
        size: TerminalSize,
        left: StatsSnapshot,
        right: StatsSnapshot,
        originalQuery: String,
        lightningQuery: String,
        elapsedSecs: Double,
        recordsSeeded: Long,
        seederError: String?,
    ) {
        val w = size.columns
        val h = size.rows
        val midCol = w / 2

        drawTitleBar(tg, w, elapsedSecs, recordsSeeded)
        drawSeederError(tg, w, seederError)
        drawPanelHeaders(tg, w, midCol)
        drawDividerColumn(tg, midCol, h)

        var row = 4
        row = drawStats(tg, 2, row, left)
        drawStats(tg, midCol + 2, 4, right)

        drawHorizontalSeparator(tg, w, row)
        row += 1

        // Draw speedup indicator between the stats and query labels
        drawSpeedupIndicator(tg, w, row, left, right)
        row += 1

        drawHorizontalSeparator(tg, w, row)
        row += 1

        drawQueryLabels(tg, w, midCol, row, originalQuery, lightningQuery)
        row += 1

        drawHorizontalSeparator(tg, w, row)
        row += 1

        val maxDataRows = h - row - 2
        drawDataTable(tg, 1, row, midCol - 2, left, maxDataRows)
        drawDataTable(tg, midCol + 2, row, w - midCol - 3, right, maxDataRows)

        drawFooter(tg, w, h)
    }

    private fun drawTitleBar(
        tg: TextGraphics,
        width: Int,
        elapsedSecs: Double,
        recordsSeeded: Long,
    ) {
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.backgroundColor = TextColor.ANSI.BLUE
        tg.enableModifiers(SGR.BOLD)

        val title = " LIGHTNING TABLES  -  Live Benchmark "
        val elapsed = formatElapsed(elapsedSecs)
        val seeded = "%,d".format(recordsSeeded)
        val rightInfo = "Seeded: $seeded  |  $elapsed "
        val gap = maxOf(0, width - title.length - rightInfo.length)

        tg.putString(0, 0, title + " ".repeat(gap) + rightInfo)
        tg.disableModifiers(SGR.BOLD)
        tg.backgroundColor = TextColor.ANSI.DEFAULT
    }

    private fun drawSeederError(tg: TextGraphics, width: Int, seederError: String?) {
        if (seederError != null) {
            tg.foregroundColor = TextColor.ANSI.RED
            tg.putString(1, 1, "Seeder error: ${truncate(seederError, width - 2)}")
            tg.foregroundColor = TextColor.ANSI.DEFAULT
        }
    }

    private fun drawPanelHeaders(tg: TextGraphics, width: Int, midCol: Int) {
        val row = 2

        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.backgroundColor = TextColor.ANSI.MAGENTA
        tg.enableModifiers(SGR.BOLD)

        val lHeader = centerPad("ORIGINAL QUERY", midCol)
        tg.putString(0, row, lHeader)
        tg.backgroundColor = TextColor.ANSI.DEFAULT
        tg.putString(midCol, row, " ")

        tg.backgroundColor = TextColor.ANSI.GREEN
        tg.foregroundColor = TextColor.ANSI.BLACK
        val rHeader = centerPad("LIGHTNING TABLE", width - midCol - 1)
        tg.putString(midCol + 1, row, rHeader)

        tg.backgroundColor = TextColor.ANSI.DEFAULT
        tg.disableModifiers(SGR.BOLD)
        tg.foregroundColor = TextColor.ANSI.DEFAULT
    }

    private fun drawDividerColumn(tg: TextGraphics, midCol: Int, height: Int) {
        tg.foregroundColor = TextColor.ANSI.WHITE
        for (r in 3 until height - 1) {
            tg.putString(midCol, r, "|")
        }
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
        tg.putString(col, r++, "Avg        : ${"%.2f".format(snap.avgTimeMs)} ms")
        tg.putString(col, r++, "Min        : ${fmtMs(snap.minTimeMs)}")
        tg.putString(col, r++, "Max        : ${fmtMs(snap.maxTimeMs)}")
        tg.putString(col, r++, "Rows       : ${snap.rowCount}")

        // Add adaptive throughput metric
        if (snap.avgTimeMs > 0 && snap.totalRuns > 0) {
            val throughput = formatThroughput(snap.avgTimeMs)
            tg.putString(col, r++, "Throughput : $throughput")
        }

        return r
    }

    private fun drawSpeedupIndicator(
        tg: TextGraphics,
        width: Int,
        row: Int,
        left: StatsSnapshot,
        right: StatsSnapshot,
    ) {
        if (left.avgTimeMs > 0 && right.avgTimeMs > 0 && left.totalRuns > 0 && right.totalRuns > 0) {
            val speedup = left.avgTimeMs / right.avgTimeMs
            val savedMs = left.avgTimeMs - right.avgTimeMs

            val text = when {
                speedup > 1.1 -> {
                    val speedupStr = "%.1f".format(speedup)
                    val savedStr = if (savedMs >= 1000) {
                        "${"%.2f".format(savedMs / 1000)}s"
                    } else {
                        "${savedMs.toInt()}ms"
                    }
                    "⚡ ${speedupStr}x faster | Saving $savedStr per query"
                }
                speedup < 0.9 -> {
                    val slowerBy = "%.1f".format(1 / speedup)
                    "⚠ ${slowerBy}x slower"
                }
                else -> "≈ Similar performance"
            }

            tg.foregroundColor = when {
                speedup > 2.0 -> TextColor.ANSI.GREEN
                speedup > 1.1 -> TextColor.ANSI.YELLOW
                else -> TextColor.ANSI.RED
            }
            tg.enableModifiers(SGR.BOLD)
            val startCol = maxOf(0, (width - text.length) / 2)
            tg.putString(startCol, row, text)
            tg.disableModifiers(SGR.BOLD)
            tg.foregroundColor = TextColor.ANSI.DEFAULT
        }
    }

    private fun drawHorizontalSeparator(tg: TextGraphics, width: Int, row: Int) {
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.putString(0, row, "-".repeat(width))
        tg.foregroundColor = TextColor.ANSI.DEFAULT
    }

    private fun drawQueryLabels(
        tg: TextGraphics,
        width: Int,
        midCol: Int,
        row: Int,
        originalQuery: String,
        lightningQuery: String,
    ) {
        tg.foregroundColor = TextColor.ANSI.WHITE
        tg.putString(
            2,
            row,
            "Left:  " + truncate(originalQuery.replace("\n", " ").trim(), midCol - 10)
        )
        tg.putString(
            midCol + 2,
            row,
            "Right: " + truncate(lightningQuery, width - midCol - 12)
        )
        tg.foregroundColor = TextColor.ANSI.DEFAULT
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
            tg.putString(col + i * colWidth, r, fitStr(name, colWidth))
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
                tg.putString(col + i * colWidth, r, fitStr(value, colWidth))
            }
            r++
        }

        if (snap.rows.size > displayRows.size) {
            tg.foregroundColor = TextColor.ANSI.WHITE
            tg.putString(col, r, "... ${snap.rows.size - displayRows.size} more rows")
            tg.foregroundColor = TextColor.ANSI.DEFAULT
        }
    }

    private fun drawFooter(tg: TextGraphics, width: Int, height: Int) {
        tg.foregroundColor = TextColor.ANSI.WHITE
        val footer = "Press 'S' to seed 10 records  |  Press ENTER or 'q' to stop"
        tg.putString(maxOf(0, (width - footer.length) / 2), height - 1, footer)
        tg.foregroundColor = TextColor.ANSI.DEFAULT
    }

    // Helper functions
    private fun formatElapsed(secs: Double): String {
        val totalSecs = secs.toLong()
        val h = totalSecs / 3600
        val m = (totalSecs % 3600) / 60
        val s = totalSecs % 60
        return "%02d:%02d:%02d".format(h, m, s)
    }

    private fun formatThroughput(avgTimeMs: Double): String {
        return when {
            avgTimeMs < 1000 -> "${"%.1f".format(1000.0 / avgTimeMs)}/sec"
            avgTimeMs < 60000 -> "${"%.1f".format(60000.0 / avgTimeMs)}/min"
            else -> "${"%.1f".format(3600000.0 / avgTimeMs)}/hr"
        }
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
