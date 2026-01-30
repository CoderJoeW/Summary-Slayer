package com.coderjoe.services

import com.coderjoe.database.DatabaseConfig
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.SelectItem
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

private data class UpsertComponents(
    val keyColumns: List<String>,
    val keyOldExpressions: List<String>,
    val keyNewExpressions: List<String>,
    val newInsertColumns: List<String>,
    val newInsertValues: List<String>,
    val newUpdateExpressions: List<String>,
    val oldInsertColumns: List<String>,
    val oldInsertValues: List<String>,
    val oldUpdateExpressions: List<String>
)

private data class ParsedQuery(
    val baseTableName: String,
    val whereClause: String?,
    val groupByColumns: List<String>,
    val aggregates: List<AggregateInfo>,
    val passThroughColumns: List<PassColumnInfo>
)

class SummaryTriggerGeneratorSqlParser {

    fun generate(query: String, summaryTable: String? = null): TriggerGeneratorResult {
        val normalizedQuery = normalizeQuery(query)
        val plainSelect = parseAndValidateQuery(normalizedQuery)
        val parsedQuery = extractQueryComponents(plainSelect, normalizedQuery)

        val columnDefinitions = loadColumnDefinitionsIfNeeded(parsedQuery.baseTableName, parsedQuery.groupByColumns, parsedQuery.aggregates)
        val aggregateColumnTypes = loadAggregateColumnTypes(parsedQuery.baseTableName, parsedQuery.aggregates)
        val summaryTableName = summaryTable ?: generateSummaryTableName(parsedQuery.baseTableName, parsedQuery.groupByColumns)
        val tableDdl = buildSummaryTableDDL(summaryTableName, columnDefinitions, parsedQuery.aggregates, aggregateColumnTypes)

        val wherePredicates = buildWherePredicates(parsedQuery.whereClause)
        val upsertComponents = buildUpsertComponents(columnDefinitions, parsedQuery.aggregates)
        val triggers = buildTriggers(parsedQuery.baseTableName, summaryTableName, wherePredicates, upsertComponents)

        return TriggerGeneratorResult(
            summaryTable = tableDdl,
            triggers = triggers,
            preview = formatPreview(tableDdl, triggers)
        )
    }

    private fun normalizeQuery(query: String): String {
        return query.trim().trimEnd(';', ' ', '\t', '\n', '\r') + ";"
    }

    private fun parseAndValidateQuery(query: String): PlainSelect {
        val statement = parseQuery(query)
        validateSelectStatement(statement)
        return extractPlainSelect(statement)
    }

    private fun parseQuery(query: String): net.sf.jsqlparser.statement.Statement {
        return try {
            CCJSqlParserUtil.parse(query)
        } catch (e: Exception) {
            throw IllegalArgumentException("Failed to parse SQL: ${e.message}", e)
        }
    }

    private fun validateSelectStatement(statement: net.sf.jsqlparser.statement.Statement) {
        if (statement !is Select) {
            throw IllegalArgumentException("Query must be a SELECT statement.")
        }
    }

    private fun extractPlainSelect(statement: net.sf.jsqlparser.statement.Statement): PlainSelect {
        @Suppress("DEPRECATION")
        return (statement as Select).selectBody as? PlainSelect
            ?: throw IllegalArgumentException("Only simple SELECT queries are supported.")
    }

    private fun extractQueryComponents(plainSelect: PlainSelect, originalQuery: String): ParsedQuery {
        val baseTableName = extractBaseTableName(plainSelect)
        val whereClause = extractWhereClause(originalQuery)
        val groupByColumns = extractGroupByColumns(plainSelect)
        val (aggregates, passThroughColumns) = extractSelectListComponents(plainSelect, groupByColumns)

        return ParsedQuery(baseTableName, whereClause, groupByColumns, aggregates, passThroughColumns)
    }

    private fun extractBaseTableName(plainSelect: PlainSelect): String {
        val fromItem = plainSelect.fromItem
            ?: throw IllegalArgumentException("Query must contain FROM clause.")

        if (fromItem !is SqlTable) {
            throw IllegalArgumentException("Exactly one base table is supported.")
        }

        val tableName = fromItem.name
        if (tableName.isNullOrEmpty()) {
            throw IllegalArgumentException("Could not resolve base table name.")
        }

        return tableName
    }

    private fun extractGroupByColumns(plainSelect: PlainSelect): List<String> {
        val groupColumns = mutableListOf<String>()

        plainSelect.groupBy?.let { groupBy ->
            @Suppress("DEPRECATION")
            groupBy.groupByExpressions?.forEach { expr ->
                if (expr is Column) {
                    groupColumns.add(trimIdentifier(expr.columnName))
                } else {
                    throw IllegalArgumentException("Only simple column GROUP BY expressions are supported.")
                }
            }
        }

        return groupColumns
    }

    private fun extractSelectListComponents(plainSelect: PlainSelect, groupByColumns: List<String>): Pair<List<AggregateInfo>, List<PassColumnInfo>> {
        val aggregates = mutableListOf<AggregateInfo>()
        val passThroughColumns = mutableListOf<PassColumnInfo>()

        plainSelect.selectItems?.forEach { selectItem ->
            processSelectItem(selectItem, groupByColumns, aggregates, passThroughColumns)
        }

        validateAggregatesPresent(aggregates)
        return Pair(aggregates, passThroughColumns)
    }

    private fun processSelectItem(
        selectItem: Any,
        groupByColumns: List<String>,
        aggregates: MutableList<AggregateInfo>,
        passThroughColumns: MutableList<PassColumnInfo>
    ) {
        when (selectItem) {
            is SelectItem<*> -> {
                val expression = selectItem.expression
                val alias = selectItem.alias?.name

                when (expression) {
                    is Column -> processColumnExpression(expression, alias, groupByColumns, passThroughColumns)
                    is Function -> processFunctionExpression(expression, alias, aggregates)
                    else -> throw IllegalArgumentException("Unsupported SELECT expression type: ${expression.javaClass.simpleName}")
                }
            }
            else -> throw IllegalArgumentException("Unsupported SELECT item type.")
        }
    }

    private fun processColumnExpression(
        column: Column,
        alias: String?,
        groupByColumns: List<String>,
        passThroughColumns: MutableList<PassColumnInfo>
    ) {
        val columnName = trimIdentifier(column.columnName)
        if (!groupByColumns.contains(columnName)) {
            throw IllegalArgumentException("Non-aggregate column \"$columnName\" must be included in GROUP BY.")
        }
        passThroughColumns.add(PassColumnInfo(columnName, alias ?: columnName))
    }

    private fun processFunctionExpression(function: Function, alias: String?, aggregates: MutableList<AggregateInfo>) {
        val functionName = function.name.uppercase()
        validateAggregateFunction(functionName)

        val argument = extractAggregateArgument(function)
        validateAggregateArgument(functionName, argument)

        val aggregateAlias = alias ?: generateDefaultAggregateAlias(functionName, argument)
        aggregates.add(AggregateInfo(functionName, argument, aggregateAlias))
    }

    private fun validateAggregateFunction(functionName: String) {
        if (functionName !in listOf("SUM", "COUNT")) {
            throw IllegalArgumentException("Aggregate $functionName is not supported (only SUM, COUNT).")
        }
    }

    private fun extractAggregateArgument(function: Function): String {
        @Suppress("DEPRECATION")
        val parameters = function.parameters?.expressions
        if (parameters.isNullOrEmpty()) {
            throw IllegalArgumentException("Malformed aggregate function.")
        }

        val argument = parameters[0]
        return when {
            argument is Column -> trimIdentifier(argument.columnName)
            argument.toString().trim() == "*" -> "*"
            else -> throw IllegalArgumentException("Unsupported aggregate argument (use column for SUM, * for COUNT).")
        }
    }

    private fun validateAggregateArgument(functionName: String, argument: String) {
        if (functionName == "COUNT" && argument != "*") {
            throw IllegalArgumentException("Only COUNT(*) is supported.")
        }
    }

    private fun generateDefaultAggregateAlias(functionName: String, argument: String): String {
        return if (functionName == "SUM") "sum_$argument" else "row_count"
    }

    private fun validateAggregatesPresent(aggregates: List<AggregateInfo>) {
        if (aggregates.isEmpty()) {
            throw IllegalArgumentException("At least one aggregate (SUM/COUNT) is required.")
        }
    }

    private fun loadColumnDefinitionsIfNeeded(baseTableName: String, groupByColumns: List<String>, aggregates: List<AggregateInfo>): Map<String, String> {
        if (groupByColumns.isEmpty()) {
            return emptyMap()
        }

        val connection = DatabaseConfig.getConnection()

        return loadColumnDefinitions(connection.catalog, baseTableName, groupByColumns)
    }

    private fun loadAggregateColumnTypes(baseTableName: String, aggregates: List<AggregateInfo>): Map<String, String> {
        val connection = DatabaseConfig.getConnection()

        val sumColumns = aggregates.filter { it.func == "SUM" && it.col != "*" }.map { it.col }
        if (sumColumns.isEmpty()) {
            return emptyMap()
        }

        return loadColumnTypes(connection.catalog, baseTableName, sumColumns)
    }

    private fun loadColumnTypes(databaseName: String, tableName: String, columnNames: List<String>): Map<String, String> {
        val connection = DatabaseConfig.getConnection()

        val placeholders = columnNames.joinToString(",") { "?" }
        val sql = """
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME IN ($placeholders)
        """.trimIndent()

        val statement = connection.prepareStatement(sql)
        statement.setString(1, databaseName)
        statement.setString(2, tableName)
        columnNames.forEachIndexed { index, columnName ->
            statement.setString(index + 3, columnName)
        }

        val types = mutableMapOf<String, String>()
        val resultSet = statement.executeQuery()

        while (resultSet.next()) {
            val columnName = resultSet.getString("COLUMN_NAME")
            val columnType = resultSet.getString("COLUMN_TYPE")
            types[columnName] = columnType
        }

        resultSet.close()
        statement.close()

        return types
    }

    private fun loadColumnDefinitions(databaseName: String, tableName: String, columnNames: List<String>): Map<String, String> {
        val connection = DatabaseConfig.getConnection()

        val columnDefinitionsMap = queryColumnDefinitions(connection, databaseName, tableName, columnNames)
        validateAllColumnsFound(columnNames, columnDefinitionsMap, tableName)

        return columnNames.associateWith { columnDefinitionsMap[it]!! }
    }

    private fun queryColumnDefinitions(
        connection: java.sql.Connection,
        databaseName: String,
        tableName: String,
        columnNames: List<String>
    ): Map<String, String> {
        val placeholders = columnNames.joinToString(",") { "?" }
        val sql = """
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME IN ($placeholders)
        """.trimIndent()

        val statement = connection.prepareStatement(sql)
        statement.setString(1, databaseName)
        statement.setString(2, tableName)
        columnNames.forEachIndexed { index, columnName ->
            statement.setString(index + 3, columnName)
        }

        val definitions = mutableMapOf<String, String>()
        val resultSet = statement.executeQuery()

        while (resultSet.next()) {
            val columnName = resultSet.getString("COLUMN_NAME")
            val columnType = resultSet.getString("COLUMN_TYPE")
            val isNullable = resultSet.getString("IS_NULLABLE")

            definitions[columnName] = "`$columnName` $columnType ${if (isNullable == "YES") "NULL" else "NOT NULL"}"
        }

        resultSet.close()
        statement.close()

        return definitions
    }

    private fun validateAllColumnsFound(columnNames: List<String>, definitions: Map<String, String>, tableName: String) {
        columnNames.forEach { columnName ->
            if (!definitions.containsKey(columnName)) {
                throw IllegalArgumentException("Group-by column `$columnName` not found on `$tableName`.")
            }
        }
    }

    private fun generateSummaryTableName(baseTableName: String, groupByColumns: List<String>): String {
        return if (groupByColumns.isEmpty()) {
            convertToSnakeCase("${baseTableName}_summary")
        } else {
            convertToSnakeCase("${baseTableName}_${groupByColumns.joinToString("_")}_summary")
        }
    }

    private fun buildSummaryTableDDL(
        summaryTableName: String,
        keyColumnDefinitions: Map<String, String>,
        aggregates: List<AggregateInfo>,
        aggregateColumnTypes: Map<String, String>
    ): String {
        val (allColumnDefinitions, primaryKeyColumns) = if (keyColumnDefinitions.isEmpty()) {
            buildNonGroupedTableStructure(aggregates, aggregateColumnTypes)
        } else {
            buildGroupedTableStructure(keyColumnDefinitions, aggregates, aggregateColumnTypes)
        }

        return formatTableDdl(summaryTableName, allColumnDefinitions, primaryKeyColumns)
    }

    private fun buildNonGroupedTableStructure(aggregates: List<AggregateInfo>, aggregateColumnTypes: Map<String, String>): Pair<List<String>, String> {
        val columnDefinitions = listOf("`summary_id` TINYINT UNSIGNED NOT NULL DEFAULT 1") +
            aggregates.map { buildAggregateColumnDefinition(it, aggregateColumnTypes) }
        return Pair(columnDefinitions, "`summary_id`")
    }

    private fun buildGroupedTableStructure(
        keyColumnDefinitions: Map<String, String>,
        aggregates: List<AggregateInfo>,
        aggregateColumnTypes: Map<String, String>
    ): Pair<List<String>, String> {
        val columnDefinitions = keyColumnDefinitions.values.toList() +
            aggregates.map { buildAggregateColumnDefinition(it, aggregateColumnTypes) }
        val primaryKeyColumns = keyColumnDefinitions.keys.joinToString(",") { "`$it`" }
        return Pair(columnDefinitions, primaryKeyColumns)
    }

    private fun buildAggregateColumnDefinition(aggregate: AggregateInfo, aggregateColumnTypes: Map<String, String>): String {
        return if (aggregate.func == "SUM") {
            val columnType = aggregateColumnTypes[aggregate.col] ?: "DECIMAL(38,6)"
            "`${aggregate.alias}` $columnType NOT NULL DEFAULT 0"
        } else {
            "`${aggregate.alias}` BIGINT UNSIGNED NOT NULL DEFAULT 0"
        }
    }

    private fun formatTableDdl(tableName: String, columnDefinitions: List<String>, primaryKeyColumns: String): String {
        return """CREATE TABLE IF NOT EXISTS `$tableName` (
  ${columnDefinitions.joinToString(",\n  ")},
  PRIMARY KEY ($primaryKeyColumns)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""
    }

    private fun buildWherePredicates(whereClause: String?): Pair<String, String> {
        val oldRowPredicate = if (whereClause != null) prefixPredicateWithRowReference(whereClause, "OLD") else "1"
        val newRowPredicate = if (whereClause != null) prefixPredicateWithRowReference(whereClause, "NEW") else "1"
        return Pair(oldRowPredicate, newRowPredicate)
    }

    private fun buildUpsertComponents(
        columnDefinitions: Map<String, String>,
        aggregates: List<AggregateInfo>
    ): UpsertComponents {
        val (keyColumns, keyOldExpressions, keyNewExpressions) = buildKeyExpressions(columnDefinitions)
        val (newInsertColumns, newInsertValues, newUpdateExpressions) = buildNewRowExpressions(aggregates)
        val (oldInsertColumns, oldInsertValues, oldUpdateExpressions) = buildOldRowExpressions(aggregates)

        return UpsertComponents(
            keyColumns, keyOldExpressions, keyNewExpressions,
            newInsertColumns, newInsertValues, newUpdateExpressions,
            oldInsertColumns, oldInsertValues, oldUpdateExpressions
        )
    }

    private fun buildKeyExpressions(columnDefinitions: Map<String, String>): Triple<List<String>, List<String>, List<String>> {
        return if (columnDefinitions.isEmpty()) {
            Triple(listOf("`summary_id`"), listOf("1"), listOf("1"))
        } else {
            Triple(
                columnDefinitions.keys.map { "`$it`" },
                columnDefinitions.keys.map { "OLD.`$it`" },
                columnDefinitions.keys.map { "NEW.`$it`" }
            )
        }
    }

    private fun buildNewRowExpressions(aggregates: List<AggregateInfo>): Triple<List<String>, List<String>, List<String>> {
        val insertColumns = mutableListOf<String>()
        val insertValues = mutableListOf<String>()
        val updateExpressions = mutableListOf<String>()

        aggregates.forEach { aggregate ->
            val (column, value) = buildNewRowAggregateExpression(aggregate)
            insertColumns.add(column)
            insertValues.add(value)
            updateExpressions.add("$column = $column + VALUES($column)")
        }

        return Triple(insertColumns, insertValues, updateExpressions)
    }

    private fun buildNewRowAggregateExpression(aggregate: AggregateInfo): Pair<String, String> {
        val columnName = "`${aggregate.alias}`"
        val value = if (aggregate.func == "SUM") "NEW.`${aggregate.col}`" else "1"
        return Pair(columnName, value)
    }

    private fun buildOldRowExpressions(aggregates: List<AggregateInfo>): Triple<List<String>, List<String>, List<String>> {
        val insertColumns = mutableListOf<String>()
        val insertValues = mutableListOf<String>()
        val updateExpressions = mutableListOf<String>()

        aggregates.forEach { aggregate ->
            val (column, value) = buildOldRowAggregateExpression(aggregate)
            insertColumns.add(column)
            insertValues.add(value)
            updateExpressions.add("$column = $column + VALUES($column)")
        }

        return Triple(insertColumns, insertValues, updateExpressions)
    }

    private fun buildOldRowAggregateExpression(aggregate: AggregateInfo): Pair<String, String> {
        val columnName = "`${aggregate.alias}`"
        val value = if (aggregate.func == "SUM") "-(OLD.`${aggregate.col}`)" else "-1"
        return Pair(columnName, value)
    }

    private fun buildTriggers(
        baseTableName: String,
        summaryTableName: String,
        wherePredicates: Pair<String, String>,
        upsertComponents: UpsertComponents
    ): Map<String, String> {
        val (oldRowPredicate, newRowPredicate) = wherePredicates

        val oldUpsertStatement = TriggerGenerator().buildUpsertStatement(
            summaryTableName,
            upsertComponents.keyColumns,
            upsertComponents.keyOldExpressions,
            upsertComponents.oldInsertColumns,
            upsertComponents.oldInsertValues,
            upsertComponents.oldUpdateExpressions
        )

        val newUpsertStatement = TriggerGenerator().buildUpsertStatement(
            summaryTableName,
            upsertComponents.keyColumns,
            upsertComponents.keyNewExpressions,
            upsertComponents.newInsertColumns,
            upsertComponents.newInsertValues,
            upsertComponents.newUpdateExpressions
        )

        val sanitizedTableName = sanitizeIdentifier(baseTableName)

        return mapOf(
            "insert" to TriggerGenerator().buildInsertTrigger(sanitizedTableName, baseTableName, newRowPredicate, newUpsertStatement),
            "update" to TriggerGenerator().buildUpdateTrigger(sanitizedTableName, baseTableName, oldRowPredicate, oldUpsertStatement, newRowPredicate, newUpsertStatement),
            "delete" to TriggerGenerator().buildDeleteTrigger(sanitizedTableName, baseTableName, oldRowPredicate, oldUpsertStatement)
        )
    }

    private fun formatPreview(tableDdl: String, triggers: Map<String, String>): String {
        return """-- Summary table to create:
$tableDdl

-- Triggers to create:
${triggers["insert"]}

${triggers["update"]}

${triggers["delete"]}"""
    }

    private fun trimIdentifier(identifier: String): String {
        var result = identifier.trim().trim('`', '"')
        if (result.contains('.')) {
            result = result.split('.').last().trim('`', '"')
        }
        return result
    }

    private fun extractWhereClause(query: String): String? {
        val whereBeforeGroupBy = extractWhereBeforeGroupBy(query)
        if (whereBeforeGroupBy != null) {
            return whereBeforeGroupBy
        }

        return extractWhereAtEnd(query)
    }

    private fun extractWhereBeforeGroupBy(query: String): String? {
        val pattern = Regex("""\bWHERE\b(.*?)\bGROUP\s+BY\b""", RegexOption.IGNORE_CASE)
        val match = pattern.find(query)
        return match?.groupValues?.get(1)?.trim()
    }

    private fun extractWhereAtEnd(query: String): String? {
        if (!query.uppercase().contains(" WHERE ")) {
            return null
        }

        val pattern = Regex("""\bWHERE\b(.*?)$""", RegexOption.IGNORE_CASE)
        val match = pattern.find(query)
        return match?.groupValues?.get(1)?.trim()?.trimEnd(';', ' ', '\t', '\n', '\r')
    }

    private fun prefixPredicateWithRowReference(expression: String, rowReference: String): String {
        val withoutTableQualifiers = removeTableQualifiers(expression)
        val withPrefixedIdentifiers = prefixColumnIdentifiers(withoutTableQualifiers, rowReference)
        return "($withPrefixedIdentifiers)"
    }

    private fun removeTableQualifiers(expression: String): String {
        return expression.replace(Regex("""`?(\w+)`?\s*\."""), "")
    }

    private fun prefixColumnIdentifiers(expression: String, prefix: String): String {
        val sqlKeywords = setOf("AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "END", "TRUE", "FALSE")
        val pattern = Regex("""`?([A-Za-z_][A-Za-z0-9_]*)`(?=\s*(=|<>|!=|<|>|<=|>=|IS|IN|LIKE|BETWEEN|\)|\s|${'$'}))""", RegexOption.IGNORE_CASE)

        return pattern.replace(expression) { matchResult ->
            val identifier = matchResult.groupValues[1].uppercase()
            if (identifier in sqlKeywords) {
                matchResult.value
            } else {
                "$prefix.`${matchResult.groupValues[1]}`"
            }
        }
    }

    private fun convertToSnakeCase(text: String): String {
        return text.replace(Regex("([a-z])([A-Z])"), "$1_$2")
            .lowercase()
            .replace(Regex("[^a-z0-9_]"), "_")
    }

    private fun sanitizeIdentifier(identifier: String): String {
        return identifier.replace(Regex("[^a-zA-Z0-9_]"), "_")
    }
}


