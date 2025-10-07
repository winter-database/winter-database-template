package io.github.winter.database.template.refresh;

import io.github.winter.boot.sql.Preconditions;
import io.github.winter.database.table.TableNameReader;
import io.github.winter.database.table.TableSchema;
import io.github.winter.database.table.TableSchemaReader;
import io.github.winter.database.template.TableSchemaRegistry;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * 刷表概要
 *
 * @author changebooks@qq.com
 */
public class RefreshTableSchemaListener implements ApplicationListener<RefreshTableSchemaEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RefreshTableSchemaListener.class);

    /**
     * the {@link ApplicationEventPublisher} instance
     */
    private final ApplicationEventPublisher publisher;

    /**
     * the {@link JdbcTemplate} instance
     */
    private final JdbcTemplate jdbcTemplate;

    public RefreshTableSchemaListener(ApplicationEventPublisher publisher, JdbcTemplate jdbcTemplate) {
        Preconditions.requireNonNull(publisher, "publisher must not be null");
        Preconditions.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");

        this.publisher = publisher;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void onApplicationEvent(RefreshTableSchemaEvent event) {
        String tableName = event.getTableName();
        if (tableName == null) {
            refreshAll();
        } else {
            refresh(tableName);
        }
    }

    /**
     * Publish Refresh All
     */
    public void publishRefreshAll() {
        Thread.ofVirtual().start(this::doPublishRefreshAll);
    }

    /**
     * Publish Refresh
     *
     * @param tableName Table Name
     */
    public void publishRefresh(String tableName) {
        Thread.ofVirtual().start(() -> doPublishRefresh(tableName));
    }

    /**
     * Publish Refresh All
     */
    protected void doPublishRefreshAll() {
        try {
            ApplicationEventPublisher publisher = getPublisher();
            publisher.publishEvent(new RefreshTableSchemaEvent());
        } catch (Throwable ex) {
            LOGGER.error("doPublishRefreshAll failed, throwable: ", ex);
        }
    }

    /**
     * Publish Refresh
     *
     * @param tableName Table Name
     */
    protected void doPublishRefresh(String tableName) {
        try {
            ApplicationEventPublisher publisher = getPublisher();
            publisher.publishEvent(new RefreshTableSchemaEvent(tableName));
        } catch (Throwable ex) {
            LOGGER.error("doPublishRefresh failed, tableName: {}, throwable: ", tableName, ex);
        }
    }

    /**
     * Refresh All
     */
    public void refreshAll() {
        Connection connection = doGetConnection();
        if (connection == null) {
            LOGGER.error("refreshAll failed, connection must not be null");
            return;
        }

        List<String> tableNames = doGetTableNames(connection);
        if (tableNames == null) {
            return;
        }

        removeAll(tableNames);

        for (String tableName : tableNames) {
            if (tableName != null) {
                doRefresh(connection, tableName);
            }
        }
    }

    /**
     * Remove All
     *
     * @param tableNames [ Table Name ]
     */
    public void removeAll(List<String> tableNames) {
        if (tableNames == null) {
            return;
        }

        List<String> removeTableNames = TableSchemaRegistry.getTableNames()
                .stream()
                .filter(Objects::nonNull)
                .filter(Predicate.not(tableNames::contains))
                .toList();
        if (removeTableNames.isEmpty()) {
            return;
        }

        for (String tableName : removeTableNames) {
            if (tableName != null) {
                TableSchemaRegistry.remove(tableName);
            }
        }
    }

    /**
     * Refresh
     *
     * @param tableName Table Name
     */
    public void refresh(String tableName) {
        Connection connection = doGetConnection();
        if (connection != null) {
            doRefresh(connection, tableName);
        } else {
            LOGGER.error("refresh failed, connection must not be null, tableName: {}", tableName);
        }
    }

    /**
     * Refresh Schema
     *
     * @param connection the {@link Connection} instance
     * @param tableName  Table Name
     */
    protected void doRefresh(Connection connection, String tableName) {
        try {
            Preconditions.requireNonNull(connection, "connection must not be null, tableName: " + tableName);
            Preconditions.requireNonNull(tableName, "tableName must not be null");

            doRegistry(connection, tableName);
            LOGGER.info("doRefresh trace, tableName: {}", tableName);
        } catch (Throwable ex) {
            LOGGER.error("doRefresh failed, tableName: {}, throwable: ", tableName, ex);
        }
    }

    /**
     * Registry Schema
     *
     * @param connection the {@link Connection} instance
     * @param tableName  Table Name
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs
     *                      or
     *                      this method is called on a closed result set
     */
    protected void doRegistry(@NotNull Connection connection, @NotNull String tableName) throws SQLException {
        String trimmedName = tableName.trim();
        Preconditions.requireNonEmpty(trimmedName, "tableName must not be empty");

        TableSchema tableSchema = TableSchemaReader.read(connection, trimmedName);
        Preconditions.requireNonNull(tableSchema, "unsupported tableName: " + trimmedName + ", dbName: " + connection.getCatalog());

        TableSchemaRegistry.put(tableSchema);
    }

    /**
     * Get Table Names
     *
     * @param connection the {@link Connection} instance
     * @return [ Table Name ]
     */
    @Nullable
    protected List<String> doGetTableNames(Connection connection) {
        try {
            Preconditions.requireNonNull(connection, "connection must not be null");
            List<String> tableNames = TableNameReader.read(connection);
            if (tableNames == null) {
                LOGGER.error("doGetTableNames failed, tableNames must not be null, dbName: " + connection.getCatalog());
                return null;
            }

            if (tableNames.isEmpty()) {
                LOGGER.error("doGetTableNames failed, tableNames must not be empty, dbName: " + connection.getCatalog());
                return null;
            }

            return tableNames;
        } catch (Throwable ex) {
            LOGGER.error("doGetTableNames failed, throwable: ", ex);
            return null;
        }
    }

    /**
     * Get Connection
     *
     * @return the {@link Connection} instance
     */
    @Nullable
    protected Connection doGetConnection() {
        try {
            DataSource dataSource = doGetDataSource();
            Preconditions.requireNonNull(dataSource, "dataSource must not be null");

            return dataSource.getConnection();
        } catch (Throwable ex) {
            LOGGER.error("doGetConnection failed, throwable: ", ex);
            return null;
        }
    }

    /**
     * Get DataSource
     *
     * @return the {@link DataSource} instance
     */
    @Nullable
    protected DataSource doGetDataSource() {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return jdbcTemplate.getDataSource();
    }

    @NotNull
    public ApplicationEventPublisher getPublisher() {
        return publisher;
    }

    @NotNull
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

}
