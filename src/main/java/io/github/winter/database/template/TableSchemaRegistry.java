package io.github.winter.database.template;

import io.github.winter.boot.sql.Preconditions;
import io.github.winter.database.table.TableSchema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 寄存表概要
 *
 * @author changebooks@qq.com
 */
public final class TableSchemaRegistry {
    /**
     * [ Table Name : the {@link TableSchema} instance ]
     */
    private static final Map<String, TableSchema> DATA = new ConcurrentHashMap<>();

    private TableSchemaRegistry() {
    }

    /**
     * Get Schema
     *
     * @param tableName Table Name
     * @return the {@link TableSchema} instance
     */
    public static TableSchema get(String tableName) {
        if (tableName != null) {
            return DATA.get(tableName);
        } else {
            return null;
        }
    }

    /**
     * Put Schema
     *
     * @param tableSchema the {@link TableSchema} instance
     * @return previous {@link TableSchema} instance
     */
    public static TableSchema put(TableSchema tableSchema) {
        Preconditions.requireNonNull(tableSchema, "tableSchema must not be null");

        String tableName = tableSchema.getTableName();
        return DATA.put(tableName, tableSchema);
    }

    /**
     * Put Schema
     *
     * @param tableName   Table Name
     * @param tableSchema the {@link TableSchema} instance
     * @return previous {@link TableSchema} instance
     */
    public static TableSchema put(String tableName, TableSchema tableSchema) {
        Preconditions.requireNonNull(tableName, "tableName must not be null");
        Preconditions.requireNonNull(tableSchema, "tableSchema must not be null, tableName: " + tableName);
        return DATA.put(tableName, tableSchema);
    }

    /**
     * Remove Schema
     *
     * @param tableName Table Name
     * @return previous {@link TableSchema} instance
     */
    public static TableSchema remove(String tableName) {
        Preconditions.requireNonNull(tableName, "tableName must not be null");
        return DATA.remove(tableName);
    }

    /**
     * Contains Schema ?
     *
     * @param tableName Table Name
     * @return contains ? true : false
     */
    public static boolean contains(String tableName) {
        if (tableName != null) {
            return DATA.containsKey(tableName);
        } else {
            return false;
        }
    }

}
