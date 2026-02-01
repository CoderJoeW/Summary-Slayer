package com.coderjoe

import com.coderjoe.database.DatabaseConfig
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.sql.DriverManager

data class ColumnSpec(
    val typeName: String,
    val size: Int,
    val decimalDigits: Int?,
    val nullable: Boolean,
)

/**
 * Base class for integration tests that automatically manages a docker-compose MariaDB container.
 *
 * This class automatically:
 * - Starts the docker-compose.test.yml container before all tests
 * - Waits for the database to be healthy and ready
 * - Stops and removes the container after all tests complete
 */
abstract class DockerComposeTestBase {
    companion object {
        private const val JDBC_URL = "jdbc:mariadb://localhost:3307/lightningtables"
        private const val USERNAME = "testuser"
        private const val PASSWORD = "testpassword"
        private const val DOCKER_COMPOSE_FILE = "docker-compose.test.yml"
        private const val MAX_WAIT_SECONDS = 60

        @JvmStatic
        @BeforeAll
        fun setupDatabase() {
            println("Starting Docker Compose test environment...")

            startDockerCompose()
            waitForDatabase()
            DatabaseConfig.initialize(JDBC_URL, USERNAME, PASSWORD)

            println("Docker Compose test environment ready!")
        }

        @JvmStatic
        @AfterAll
        fun teardownDatabase() {
            println("Stopping Docker Compose test environment...")
            stopDockerCompose()
            println("Docker Compose test environment stopped!")
        }

        private fun startDockerCompose() {
            val projectRoot = File(System.getProperty("user.dir"))
            val composeFile = File(projectRoot, DOCKER_COMPOSE_FILE)

            if (!composeFile.exists()) {
                throw IllegalStateException("docker-compose.test.yml not found at: ${composeFile.absolutePath}")
            }

            val process =
                ProcessBuilder(
                    "docker-compose",
                    "-f",
                    DOCKER_COMPOSE_FILE,
                    "up",
                    "-d",
                )
                    .directory(projectRoot)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start()

            val exitCode = process.waitFor()
            if (exitCode != 0) {
                throw RuntimeException("Failed to start docker-compose. Exit code: $exitCode")
            }
        }

        private fun stopDockerCompose() {
            val projectRoot = File(System.getProperty("user.dir"))

            val process =
                ProcessBuilder(
                    "docker-compose",
                    "-f",
                    DOCKER_COMPOSE_FILE,
                    "down",
                    // Remove volumes to ensure clean state
                    "-v",
                )
                    .directory(projectRoot)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start()

            process.waitFor()
        }

        private fun waitForDatabase() {
            println("Waiting for database to be ready...")
            val startTime = System.currentTimeMillis()
            var lastException: Exception? = null

            while (System.currentTimeMillis() - startTime < MAX_WAIT_SECONDS * 1000) {
                try {
                    DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                        connection.createStatement().use { statement ->
                            statement.executeQuery("SELECT 1")
                            println("Database is ready!")
                            return
                        }
                    }
                } catch (e: Exception) {
                    lastException = e
                    Thread.sleep(1000)
                }
            }

            throw RuntimeException(
                "Database did not become ready within $MAX_WAIT_SECONDS seconds",
                lastException,
            )
        }

        fun connect(): java.sql.Connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)

        fun executeSQL(sql: String) {
            connect().use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(sql)
                }
            }
        }

        fun cleanDatabase() {
            connect().use { connection ->
                connection.createStatement().use { statement ->
                    // Drop any triggers and lightning tables left by previous tests
                    statement.execute("DROP TRIGGER IF EXISTS transactions_after_insert_lightning")
                    statement.execute("DROP TRIGGER IF EXISTS transactions_after_update_lightning")
                    statement.execute("DROP TRIGGER IF EXISTS transactions_after_delete_lightning")
                    statement.execute("DROP TABLE IF EXISTS transactions_user_id_lightning")

                    statement.execute("DELETE FROM transactions")
                    statement.execute("DELETE FROM users")
                    statement.execute("ALTER TABLE users AUTO_INCREMENT = 1")
                    statement.execute("ALTER TABLE transactions AUTO_INCREMENT = 1")
                }
            }
        }

        /**
         * Seed the database with test data using plain JDBC
         */
        fun reseedDatabase() {
            connect().use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("INSERT INTO users (first_name, last_name) VALUES ('John', 'Doe')")
                    statement.execute("INSERT INTO users (first_name, last_name) VALUES ('Jane', 'Smith')")
                    statement.execute("INSERT INTO users (first_name, last_name) VALUES ('Bob', 'Johnson')")

                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 1.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'SMS', 0.05)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'DATA', 2.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'CREDIT', 'CALL', 3.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'DATA', 4.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'CALL', 5.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'SMS', 6.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'DATA', 7.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (3, 'DEBIT', 'CALL', 8.00)")
                    statement.execute("INSERT INTO transactions (user_id, type, service, cost) VALUES (3, 'CREDIT', 'SMS', 9.30)")
                }
            }
        }
    }

    @BeforeEach
    fun cleanupBeforeTest() {
        cleanDatabase()
        reseedDatabase()
    }

    fun java.sql.Connection.getColumnSpecs(tableName: String): Map<String, ColumnSpec> {
        val columns = metaData.getColumns(null, null, tableName, null)
        return buildMap {
            while (columns.next()) {
                put(
                    columns.getString("COLUMN_NAME"),
                    ColumnSpec(
                        typeName = columns.getString("TYPE_NAME"),
                        size = columns.getInt("COLUMN_SIZE"),
                        decimalDigits = columns.getInt("DECIMAL_DIGITS").takeIf { !columns.wasNull() },
                        nullable = columns.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable,
                    ),
                )
            }
        }
    }
}
