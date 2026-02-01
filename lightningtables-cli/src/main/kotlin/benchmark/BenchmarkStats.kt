package com.coderjoe.benchmark

data class QueryStats(
    var totalRuns: Int = 0,
    var totalTimeMs: Long = 0,
    var lastTimeMs: Long = 0,
    var minTimeMs: Long = Long.MAX_VALUE,
    var maxTimeMs: Long = 0,
    var lastRows: List<List<String>> = emptyList(),
    var columnNames: List<String> = emptyList(),
)

data class StatsSnapshot(
    val totalRuns: Int = 0,
    val lastTimeMs: Long = 0,
    val avgTimeMs: Double = 0.0,
    val minTimeMs: Long = Long.MAX_VALUE,
    val maxTimeMs: Long = 0,
    val rowCount: Int = 0,
    val columnNames: List<String> = emptyList(),
    val rows: List<List<String>> = emptyList(),
)

fun snapshot(stats: QueryStats): StatsSnapshot =
    synchronized(stats) {
        StatsSnapshot(
            totalRuns = stats.totalRuns,
            lastTimeMs = stats.lastTimeMs,
            avgTimeMs =
                if (stats.totalRuns > 0) {
                    stats.totalTimeMs.toDouble() / stats.totalRuns
                } else {
                    0.0
                },
            minTimeMs = stats.minTimeMs,
            maxTimeMs = stats.maxTimeMs,
            rowCount = stats.lastRows.size,
            columnNames = stats.columnNames,
            rows = stats.lastRows,
        )
    }
