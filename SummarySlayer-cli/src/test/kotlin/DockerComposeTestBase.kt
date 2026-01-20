package com.coderjoe

import com.coderjoe.database.DatabaseConnection
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.sql.DriverManager

/**
 * Base class for integration tests that automatically manages a docker-compose MariaDB container.
 *
 * This class automatically:
 * - Starts the docker-compose.test.yml container before all tests
 * - Waits for the database to be healthy and ready
 * - Stops and removes the container after all tests complete
 *
 * Benefits:
 * - Fully automated container lifecycle management
 * - Clean test environment for each test run
 * - Suitable for both local development and CI/CD
 * - No manual container management required
 */
abstract class DockerComposeTestBase {

    companion object {
        private const val JDBC_URL = "jdbc:mariadb://localhost:3307/summaryslayer"
        private const val USERNAME = "testuser"
        private const val PASSWORD = "testpassword"
        private const val DOCKER_COMPOSE_FILE = "docker-compose.test.yml"
        private const val MAX_WAIT_SECONDS = 60

        @JvmStatic
        @BeforeAll
        fun setupDatabase() {
            println("Starting Docker Compose test environment...")

            // Start docker-compose
            startDockerCompose()

            // Wait for database to be ready
            waitForDatabase()

            // Initialize DatabaseConnection with docker-compose settings
            DatabaseConnection.initialize(
                url = JDBC_URL,
                username = USERNAME,
                password = PASSWORD
            )

            println("Docker Compose test environment ready!")
        }

        @BeforeEach
        fun cleanupBeforeTest() {
            // Clean database before each test to ensure test isolation
            cleanDatabase()
            // Optionally re-seed if you need the initial test data
            reseedDatabase()
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

            val process = ProcessBuilder(
                "docker-compose",
                "-f", DOCKER_COMPOSE_FILE,
                "up",
                "-d"
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

            val process = ProcessBuilder(
                "docker-compose",
                "-f", DOCKER_COMPOSE_FILE,
                "down",
                "-v"  // Remove volumes to ensure clean state
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
                lastException
            )
        }

        /**
         * Get the JDBC URL for the test database
         */
        fun getJdbcUrl(): String = JDBC_URL

        /**
         * Get the username for the test database
         */
        fun getUsername(): String = USERNAME

        /**
         * Get the password for the test database
         */
        fun getPassword(): String = PASSWORD

        /**
         * Execute a raw SQL statement (useful for test setup/teardown)
         */
        fun executeSQL(sql: String) {
            DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(sql)
                }
            }
        }

        /**
         * Clean all data from tables (useful for test isolation)
         * This is faster than recreating the container and maintains the schema.
         */
        fun cleanDatabase() {
            DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    // Disable foreign key checks temporarily
                    statement.execute("SET FOREIGN_KEY_CHECKS = 0")
                    statement.execute("TRUNCATE transactions")
                    statement.execute("TRUNCATE users")
                    // Re-enable foreign key checks
                    statement.execute("SET FOREIGN_KEY_CHECKS = 1")
                }
            }
        }

        /**
         * Re-seed the database with the initial test data.
         * Note: This assumes you've already cleaned the database.
         */
        fun reseedDatabase() {
            val seedSQL = Thread.currentThread().contextClassLoader.getResource("seed.sql")?.readText()
                ?: throw IllegalStateException("seed.sql not found in test resources")

            DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("SET FOREIGN_KEY_CHECKS = 0")
                    // Split and execute each statement
                    seedSQL.split(";")
                        .map { it.trim() }
                        .filter { it.isNotEmpty() && !it.startsWith("--") }
                        .forEach { sql ->
                            if (sql.isNotBlank()) {
                                statement.execute(sql)
                            }
                        }
                    statement.execute("SET FOREIGN_KEY_CHECKS = 1")
                }
            }
        }
    }
}

