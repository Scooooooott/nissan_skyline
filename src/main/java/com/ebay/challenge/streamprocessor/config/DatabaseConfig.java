package com.ebay.challenge.streamprocessor.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Locale;
import java.util.Set;

/**
 * DatabaseConfig
 */
@Slf4j
@Configuration
public class DatabaseConfig {

    private static final Set<String> JOURNAL_MODES = Set.of(
            "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF");
    private static final Set<String> SYNCHRONOUS_MODES = Set.of(
            "OFF", "NORMAL", "FULL", "EXTRA", "0", "1", "2", "3");

    @Bean
    public ApplicationRunner sqlitePragmaInitializer(
            DataSource dataSource,
            @Value("${output.database.journal-mode:WAL}") String journalMode,
            @Value("${output.database.synchronous:FULL}") String synchronous,
            @Value("${output.database.busy-timeout-ms:5000}") int busyTimeoutMs) {
        return args -> applyPragmas(dataSource, journalMode, synchronous, busyTimeoutMs);
    }

    private void applyPragmas(
            DataSource dataSource,
            String journalMode,
            String synchronous,
            int busyTimeoutMs) throws Exception {
        String normalizedJournalMode = normalize(journalMode, JOURNAL_MODES, "journal-mode");
        String normalizedSynchronous = normalize(synchronous, SYNCHRONOUS_MODES, "synchronous");
        if (busyTimeoutMs < 0) {
            throw new IllegalArgumentException("output.database.busy-timeout-ms cannot be negative");
        }

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            // journal_mode is database-level; synchronous and busy_timeout are
            // connection-level and must be applied to the connection used by the pool.
            statement.execute("PRAGMA journal_mode = " + normalizedJournalMode);
            statement.execute("PRAGMA synchronous = " + normalizedSynchronous);
            statement.execute("PRAGMA busy_timeout = " + busyTimeoutMs);
        }

        log.info("SQLite configured: journal_mode={}, synchronous={}, busy_timeout_ms={}",
                normalizedJournalMode, normalizedSynchronous, busyTimeoutMs);
    }

    private static String normalize(String value, Set<String> allowedValues, String propertyName) {
        String normalized = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
        if (!allowedValues.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported output.database." + propertyName + ": " + value);
        }
        return normalized;
    }
}
