package com.ebay.challenge.streamprocessor.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatabaseConfigTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private ApplicationArguments applicationArguments;

    private DatabaseConfig databaseConfig;

    @BeforeEach
    void setUp() {
        databaseConfig = new DatabaseConfig();
    }

    // Valid SQLite settings, apply all configured PRAGMA statements
    @Test
    void appliesNormalizedSqlitePragmas() throws Exception {
        stubJdbc();
        ApplicationRunner runner = databaseConfig.sqlitePragmaInitializer(
                dataSource, " wal ", " full ", 5000);

        runner.run(applicationArguments);

        verify(statement).execute("PRAGMA journal_mode = WAL");
        verify(statement).execute("PRAGMA synchronous = FULL");
        verify(statement).execute("PRAGMA busy_timeout = 5000");
    }

    // Unsupported journal mode, reject the database configuration
    @Test
    void rejectsUnsupportedJournalMode() {
        ApplicationRunner runner = databaseConfig.sqlitePragmaInitializer(
                dataSource, "unsupported", "FULL", 5000);

        assertThrows(IllegalArgumentException.class, () -> runner.run(applicationArguments));

        verifyNoInteractions(dataSource);
    }

    // Unsupported synchronous mode, reject the database configuration
    @Test
    void rejectsUnsupportedSynchronousMode() {
        ApplicationRunner runner = databaseConfig.sqlitePragmaInitializer(
                dataSource, "WAL", "unsupported", 5000);

        assertThrows(IllegalArgumentException.class, () -> runner.run(applicationArguments));

        verifyNoInteractions(dataSource);
    }

    // Negative busy timeout, reject the database configuration
    @Test
    void rejectsNegativeBusyTimeout() {
        ApplicationRunner runner = databaseConfig.sqlitePragmaInitializer(
                dataSource, "WAL", "FULL", -1);

        assertThrows(IllegalArgumentException.class, () -> runner.run(applicationArguments));

        verifyNoInteractions(dataSource);
    }

    // Null configuration value, reject the database configuration
    @Test
    void rejectsNullConfigurationValue() {
        ApplicationRunner runner = databaseConfig.sqlitePragmaInitializer(
                dataSource, null, "FULL", 5000);

        assertThrows(IllegalArgumentException.class, () -> runner.run(applicationArguments));

        verifyNoInteractions(dataSource);
    }

    // PRAGMA execution completed, close connection and statement resources
    @Test
    void closesJdbcResourcesAfterApplyingPragmas() throws Exception {
        stubJdbc();
        ApplicationRunner runner = databaseConfig.sqlitePragmaInitializer(
                dataSource, "WAL", "FULL", 5000);

        runner.run(applicationArguments);

        verify(statement).close();
        verify(connection).close();
    }

    // JDBC PRAGMA failure, propagate the configuration error
    @Test
    void propagatesPragmaExecutionFailure() throws Exception {
        stubJdbc();
        SQLException failure = new SQLException("pragma failed");
        when(statement.execute("PRAGMA journal_mode = WAL")).thenReturn(false);
        when(statement.execute("PRAGMA synchronous = FULL")).thenThrow(failure);
        ApplicationRunner runner = databaseConfig.sqlitePragmaInitializer(
                dataSource, "WAL", "FULL", 5000);

        SQLException actual = assertThrows(SQLException.class, () -> runner.run(applicationArguments));

        assertSame(failure, actual);
        verify(statement).close();
        verify(connection).close();
    }

    private void stubJdbc() throws Exception {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
    }
}
