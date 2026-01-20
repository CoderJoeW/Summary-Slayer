package com.coderjoe.services

class TriggerGenerator {
    fun buildUpsertStatement(
        tableName: String,
        keyColumns: List<String>,
        keyExpressions: List<String>,
        insertColumns: List<String>,
        insertValues: List<String>,
        updateExpressions: List<String>
    ): String {
        return """
            INSERT INTO $tableName (${keyColumns.joinToString(", ")}, ${insertColumns.joinToString(", ")})
            VALUES (${keyExpressions.joinToString(", ")}, ${insertValues.joinToString(", ")})
            ON DUPLICATE KEY UPDATE ${updateExpressions.joinToString(", ")};
        """.trimIndent()
    }

    fun buildInsertTrigger(
        tableName: String,
        baseTableName: String,
        predicate: String,
        upsertStatement: String
    ): String {
        return """
            CREATE TRIGGER `${tableName}_after_insert_summary`
            AFTER INSERT ON `$baseTableName`
            FOR EACH ROW
            BEGIN
                IF $predicate THEN
                    $upsertStatement
                END IF;
            END;
        """.trimIndent()
    }

    fun buildUpdateTrigger(
        tableName: String,
        baseTableName: String,
        oldPredicate: String,
        oldUpsertStatement: String,
        newPredicate: String,
        newUpsertStatement: String
    ): String {
        return """
            CREATE TRIGGER `${tableName}_after_update_summary`
            AFTER UPDATE ON `$baseTableName`
            FOR EACH ROW
            BEGIN
                IF $oldPredicate THEN
                    $oldUpsertStatement
                END IF;

                IF $newPredicate THEN
                    $newUpsertStatement
                END IF;
            END;
        """.trimIndent()
    }

    fun buildDeleteTrigger(
        tableName: String,
        baseTableName: String,
        predicate: String,
        upsertStatement: String
    ): String {
        return """
            CREATE TRIGGER `${tableName}_after_delete_summary`
            AFTER DELETE ON `$baseTableName`
            FOR EACH ROW
            BEGIN
                IF $predicate THEN
                    $upsertStatement
                END IF;
            END;
        """.trimIndent()
    }
}