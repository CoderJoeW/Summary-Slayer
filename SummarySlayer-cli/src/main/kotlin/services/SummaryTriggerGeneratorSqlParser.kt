package com.coderjoe.services

import com.coderjoe.database.DatabaseConnection
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.*
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.schema.Table as SqlTable
import net.sf.jsqlparser.expression.Function

data class TriggerGeneratorResult(
    val summaryTable: String,
    val triggers: Map<String, String>,
    val preview: String
)

data class AggregateInfo(
    val func: String,
    val col: String,
    val alias: String
)

data class PassColumnInfo(
    val col: String,
    val alias: String
)

class SummaryTriggerGeneratorSqlParser {

    /**
     * Generate summary table DDL and triggers from a SELECT query
     * @param query SELECT ... FROM base [WHERE ...] [GROUP BY ...]
     * @param summaryTable Optional override for summary table name
     * @return TriggerGeneratorResult with DDL and trigger definitions
     */
    fun generate(query: String, summaryTable: String? = null): TriggerGeneratorResult {
        val sql = query.trim().trimEnd(';', ' ', '\t', '\n', '\r') + ";"

        // Parse the SQL
        val statement = try {
            CCJSqlParserUtil.parse(sql)
        } catch (e: Exception) {
            throw IllegalArgumentException("Failed to parse SQL: ${e.message}", e)
        }

        if (statement !is Select) {
            throw IllegalArgumentException("Query must be a SELECT statement.")
        }

        @Suppress("DEPRECATION")
        val plainSelect = statement.selectBody as? PlainSelect
            ?: throw IllegalArgumentException("Only simple SELECT queries are supported.")

        // ---- 1) Validate basic structure ----
        if (plainSelect.fromItem == null) {
            throw IllegalArgumentException("Query must contain FROM clause.")
        }

        val fromItem = plainSelect.fromItem
        if (fromItem !is SqlTable) {
            throw IllegalArgumentException("Exactly one base table is supported.")
        }

        val baseTable = fromItem.name
        if (baseTable.isNullOrEmpty()) {
            throw IllegalArgumentException("Could not resolve base table name.")
        }

        // WHERE clause (we'll extract the text to preserve it)
        val whereText = extractWhereClause(sql)

        // ---- 2) Extract GROUP BY columns (optional) ----
        val groupCols = mutableListOf<String>()
        plainSelect.groupBy?.let { groupBy ->
            @Suppress("DEPRECATION")
            groupBy.groupByExpressions?.forEach { expr ->
                if (expr is Column) {
                    groupCols.add(trimIdent(expr.columnName))
                } else {
                    throw IllegalArgumentException("Only simple column GROUP BY expressions are supported.")
                }
            }
        }

        // ---- 3) Parse SELECT list into pass-through (group keys) and aggregates ----
        val aggregates = mutableListOf<AggregateInfo>()
        val passCols = mutableListOf<PassColumnInfo>()

        plainSelect.selectItems?.forEach { selectItem ->
            when (selectItem) {
                is SelectItem<*> -> {
                    val expr = selectItem.expression
                    val alias = selectItem.alias?.name

                    when (expr) {
                        is Column -> {
                            val col = trimIdent(expr.columnName)
                            if (!groupCols.contains(col)) {
                                throw IllegalArgumentException("Non-aggregate column \"$col\" must be included in GROUP BY.")
                            }
                            passCols.add(PassColumnInfo(col, alias ?: col))
                        }
                        is Function -> {
                            val func = expr.name.uppercase()
                            if (func !in listOf("SUM", "COUNT")) {
                                throw IllegalArgumentException("Aggregate $func is not supported (only SUM, COUNT).")
                            }

                            val arg = extractAggregateArg(expr)
                            if (func == "COUNT" && arg != "*") {
                                throw IllegalArgumentException("Only COUNT(*) is supported.")
                            }

                            val aggAlias = alias ?: if (func == "SUM") "sum_$arg" else "row_count"
                            aggregates.add(AggregateInfo(func, arg, aggAlias))
                        }
                        else -> throw IllegalArgumentException("Unsupported SELECT expression type: ${expr.javaClass.simpleName}")
                    }
                }
                else -> throw IllegalArgumentException("Unsupported SELECT item type.")
            }
        }

        if (aggregates.isEmpty()) {
            throw IllegalArgumentException("At least one aggregate (SUM/COUNT) is required.")
        }

        // ---- 4) Infer group-by column definitions from INFORMATION_SCHEMA ----
        val connection = DatabaseConnection.getConnection()
            ?: throw IllegalStateException("Database connection not initialized.")

        val dbName = connection.catalog
        val colDefs = if (groupCols.isNotEmpty()) {
            loadColumnDefs(dbName, baseTable, groupCols)
        } else {
            emptyMap()
        }

        // ---- 5) Build summary table DDL ----
        val summary = summaryTable ?: defaultSummaryName(baseTable, groupCols)
        val ddl = buildSummaryTableDDL(summary, colDefs, aggregates)

        // ---- 6) Build predicates for triggers ----
        val whereOldPref = if (whereText != null) prefixPredicate(whereText, "OLD") else "1"
        val whereNewPref = if (whereText != null) prefixPredicate(whereText, "NEW") else "1"

        // ---- 7) Assemble UPSERT delta statements ----
        val (keyCols, keyOldExprs, keyNewExprs) = if (colDefs.isEmpty()) {
            Triple(
                listOf("`summary_id`"),
                listOf("1"),
                listOf("1")
            )
        } else {
            Triple(
                colDefs.keys.map { "`$it`" },
                colDefs.keys.map { "OLD.`$it`" },
                colDefs.keys.map { "NEW.`$it`" }
            )
        }

        val newInsertCols = mutableListOf<String>()
        val newInsertVals = mutableListOf<String>()
        val updExprsNew = mutableListOf<String>()
        val oldInsertCols = mutableListOf<String>()
        val oldInsertVals = mutableListOf<String>()
        val updExprsOld = mutableListOf<String>()

        aggregates.forEach { ag ->
            val alias = ag.alias
            if (ag.func == "SUM") {
                val col = ag.col
                newInsertCols.add("`$alias`")
                newInsertVals.add("NEW.`$col`")
                updExprsNew.add("`$alias` = `$alias` + VALUES(`$alias`)")
                oldInsertCols.add("`$alias`")
                oldInsertVals.add("-(OLD.`$col`)")
                updExprsOld.add("`$alias` = `$alias` + VALUES(`$alias`)")
            } else { // COUNT
                newInsertCols.add("`$alias`")
                newInsertVals.add("1")
                updExprsNew.add("`$alias` = `$alias` + VALUES(`$alias`)")
                oldInsertCols.add("`$alias`")
                oldInsertVals.add("-1")
                updExprsOld.add("`$alias` = `$alias` + VALUES(`$alias`)")
            }
        }

        val oldUpsert = """INSERT INTO `$summary` (${keyCols.joinToString(", ")}, ${oldInsertCols.joinToString(", ")}) VALUES (${keyOldExprs.joinToString(", ")}, ${oldInsertVals.joinToString(", ")})
ON DUPLICATE KEY UPDATE ${updExprsOld.joinToString(", ")};"""

        val newUpsert = """INSERT INTO `$summary` (${keyCols.joinToString(", ")}, ${newInsertCols.joinToString(", ")}) VALUES (${keyNewExprs.joinToString(", ")}, ${newInsertVals.joinToString(", ")})
ON DUPLICATE KEY UPDATE ${updExprsNew.joinToString(", ")};"""

        // ---- 8) Triggers ----
        val trgBase = sanitizeIdent(baseTable)

        val insTrg = """CREATE TRIGGER `${trgBase}_ai_summary` AFTER INSERT ON `$baseTable` FOR EACH ROW
BEGIN
    IF $whereNewPref THEN
        $newUpsert
    END IF;
END;"""

        val delTrg = """CREATE TRIGGER `${trgBase}_ad_summary` AFTER DELETE ON `$baseTable` FOR EACH ROW
BEGIN
    IF $whereOldPref THEN
        $oldUpsert
    END IF;
END;"""

        val updTrg = """CREATE TRIGGER `${trgBase}_au_summary` AFTER UPDATE ON `$baseTable` FOR EACH ROW
BEGIN
    IF $whereOldPref THEN
        $oldUpsert
    END IF;
    IF $whereNewPref THEN
        $newUpsert
    END IF;
END;"""

        val preview = """-- Summary table to create:
$ddl

-- Triggers to create:
$insTrg

$updTrg

$delTrg"""

        return TriggerGeneratorResult(
            summaryTable = ddl,
            triggers = mapOf(
                "insert" to insTrg,
                "update" to updTrg,
                "delete" to delTrg
            ),
            preview = preview
        )
    }

    // ---------------- Helpers ----------------

    private fun trimIdent(s: String): String {
        var result = s.trim().trim('`', '"')
        // Drop table qualifiers: t.user_id -> user_id
        if (result.contains('.')) {
            result = result.split('.').last().trim('`', '"')
        }
        return result
    }

    private fun extractAggregateArg(func: Function): String {
        @Suppress("DEPRECATION")
        val params = func.parameters?.expressions
        if (params.isNullOrEmpty()) {
            throw IllegalArgumentException("Malformed aggregate function.")
        }

        val arg = params[0]
        return when {
            arg is Column -> trimIdent(arg.columnName)
            arg.toString().trim() == "*" -> "*"
            else -> throw IllegalArgumentException("Unsupported aggregate argument (use column for SUM, * for COUNT).")
        }
    }

    private fun extractWhereClause(sql: String): String? {
        // Simple, reliable: capture text between WHERE and GROUP BY (or end)
        val whereGroupPattern = Regex("""\bWHERE\b(.*?)\bGROUP\s+BY\b""", RegexOption.IGNORE_CASE)
        val whereMatch = whereGroupPattern.find(sql)

        if (whereMatch != null) {
            return whereMatch.groupValues[1].trim()
        }

        // WHERE present without GROUP BY - capture until end of statement
        if (sql.uppercase().contains(" WHERE ")) {
            val whereEndPattern = Regex("""\bWHERE\b(.*?)$""", RegexOption.IGNORE_CASE)
            val match = whereEndPattern.find(sql)
            if (match != null) {
                return match.groupValues[1].trim().trimEnd(';', ' ', '\t', '\n', '\r')
            }
        }

        return null
    }

    private fun loadColumnDefs(db: String, table: String, cols: List<String>): Map<String, String> {
        val connection = DatabaseConnection.getConnection()
            ?: throw IllegalStateException("Database connection not initialized.")

        val placeholders = cols.joinToString(",") { "?" }
        val sql = """
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME IN ($placeholders)
        """.trimIndent()

        val stmt = connection.prepareStatement(sql)
        stmt.setString(1, db)
        stmt.setString(2, table)
        cols.forEachIndexed { index, col ->
            stmt.setString(index + 3, col)
        }

        val defs = mutableMapOf<String, String>()
        val rs = stmt.executeQuery()

        while (rs.next()) {
            val colName = rs.getString("COLUMN_NAME")
            val colType = rs.getString("COLUMN_TYPE")
            val isNullable = rs.getString("IS_NULLABLE")

            defs[colName] = "`$colName` $colType ${if (isNullable == "YES") "NULL" else "NOT NULL"}"
        }
        rs.close()
        stmt.close()

        // Verify all columns were found
        cols.forEach { col ->
            if (!defs.containsKey(col)) {
                throw IllegalArgumentException("Group-by column `$col` not found on `$table`.")
            }
        }

        // Preserve input order
        return cols.associateWith { defs[it]!! }
    }

    private fun defaultSummaryName(baseTable: String, groupCols: List<String>): String {
        return if (groupCols.isEmpty()) {
            toSnakeCase("${baseTable}_summary")
        } else {
            toSnakeCase("${baseTable}_${groupCols.joinToString("_")}_summary")
        }
    }

    private fun toSnakeCase(str: String): String {
        return str.replace(Regex("([a-z])([A-Z])"), "$1_$2")
            .lowercase()
            .replace(Regex("[^a-z0-9_]"), "_")
    }

    private fun buildSummaryTableDDL(summary: String, keyColDefs: Map<String, String>, aggregates: List<AggregateInfo>): String {
        val keyLines = keyColDefs.values.toList()

        val aggLines = aggregates.map { ag ->
            val alias = ag.alias
            if (ag.func == "SUM") {
                "`$alias` DECIMAL(38,6) NOT NULL DEFAULT 0"
            } else { // COUNT
                "`$alias` BIGINT UNSIGNED NOT NULL DEFAULT 0"
            }
        }

        // Handle case where there are no grouping columns (no GROUP BY)
        val (allLines, pkCols) = if (keyColDefs.isEmpty()) {
            Pair(
                listOf("`summary_id` TINYINT UNSIGNED NOT NULL DEFAULT 1") + aggLines,
                "`summary_id`"
            )
        } else {
            Pair(
                keyLines + aggLines,
                keyColDefs.keys.joinToString(",") { "`$it`" }
            )
        }

        return """CREATE TABLE IF NOT EXISTS `$summary` (
  ${allLines.joinToString(",\n  ")},
  PRIMARY KEY ($pkCols)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""
    }

    private fun prefixPredicate(expr: String, prefix: String): String {
        // Prefix bare identifiers with OLD./NEW (keep strings/operators untouched).
        var e = expr

        // Remove table qualifiers first: t.col -> col
        e = e.replace(Regex("""`?(\w+)`?\s*\."""), "")

        // Prefix identifiers that look like column names followed by operators/whitespace/paren
        val deny = setOf("AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "END", "TRUE", "FALSE")

        e = Regex("""`?([A-Za-z_][A-Za-z0-9_]*)`(?=\s*(=|<>|!=|<|>|<=|>=|IS|IN|LIKE|BETWEEN|\)|\s|${'$'}))""", RegexOption.IGNORE_CASE)
            .replace(e) { matchResult ->
                val id = matchResult.groupValues[1].uppercase()
                if (id in deny) {
                    matchResult.value
                } else {
                    "$prefix.`${matchResult.groupValues[1]}`"
                }
            }

        return "($e)"
    }

    private fun sanitizeIdent(s: String): String {
        return s.replace(Regex("[^a-zA-Z0-9_]"), "_")
    }
}


