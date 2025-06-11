package io.github.winter.database.template;

import io.github.winter.boot.filter.BaseFilter;
import io.github.winter.boot.filter.Order;
import io.github.winter.boot.filter.Page;
import io.github.winter.boot.sql.Preconditions;
import io.github.winter.boot.sql.SqlParameter;
import io.github.winter.boot.sql.SqlParser;
import io.github.winter.boot.sql.parser.SqlParserImpl;
import io.github.winter.boot.tuple.Value;
import io.github.winter.database.executor.Executor;
import io.github.winter.database.table.TableSchema;
import io.github.winter.database.template.parser.KeyParser;
import io.github.winter.database.template.parser.SetParser;
import io.github.winter.database.template.value.DefaultValues;
import io.github.winter.database.template.value.PlaceholderValues;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Template
 *
 * @author changebooks@qq.com
 */
public class Template {
    /**
     * SELECT COUNT(*)
     */
    private static final String AGGREGATE = "COUNT(*) AS aggregate";

    /**
     * the {@link Executor} instance
     */
    private final Executor executor;

    /**
     * the {@link SqlParser} instance
     */
    private final SqlParser sqlParser;

    /**
     * the {@link TemplateLog} instance
     */
    private TemplateLog logWriter;

    public Template(Executor executor) {
        Preconditions.requireNonNull(executor, "executor must not be null");

        this.executor = executor;
        this.sqlParser = new SqlParserImpl();
    }

    public Template(Executor executor, SqlParser sqlParser) {
        Preconditions.requireNonNull(executor, "executor must not be null");
        Preconditions.requireNonNull(sqlParser, "sqlParser must not be null");

        this.executor = executor;
        this.sqlParser = sqlParser;
    }

    /**
     * SELECT LIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param orders    [ the {@link Order} instance ]
     * @param page      the {@link Page} instance
     * @param tableNum  Sharding Table Num
     * @return [ [ Column Name : Column Value ] ]
     */
    public List<Map<String, Value>> selectList(@NotNull String tableName,
                                               List<BaseFilter> filters, List<Order> orders, Page page, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);
        List<Map<String, Value>> result = selectList(tableSchema, filters, orders, page);

        writeLogSelectList(tableName, filters, orders, page, tableNum, result);
        return result;
    }

    /**
     * SELECT LIST
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param filters     [ the {@link BaseFilter} instance ]
     * @param orders      [ the {@link Order} instance ]
     * @param page        the {@link Page} instance
     * @return [ [ Column Name : Column Value ] ]
     */
    public List<Map<String, Value>> selectList(@NotNull TableSchema tableSchema,
                                               List<BaseFilter> filters, List<Order> orders, Page page) {
        SqlParameter sqlParameter = parseSelect(tableSchema, filters, orders, page);

        List<String> columnNames = tableSchema.getColumnNames();
        Map<String, Class<?>> valueTypes = tableSchema.getValueTypes();

        Executor executor = getExecutor();
        List<Map<String, Value>> result = executor.selectList(sqlParameter, columnNames, valueTypes);

        writeLogSelectList(tableSchema, filters, orders, page, result);
        return result;
    }

    /**
     * SELECT COUNT
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @return AGGREGATE
     */
    public long selectCount(@NotNull String tableName,
                            List<BaseFilter> filters, @Nullable Integer tableNum) {
        String joinedTableName = joinTableName(tableName, tableNum);
        Long aggregate = selectCount(joinedTableName, filters);
        long result = aggregate != null ? aggregate : 0;

        writeLogSelectCount(tableName, filters, tableNum, result);
        return result;
    }

    /**
     * SELECT COUNT
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @return AGGREGATE
     */
    public Long selectCount(@NotNull String tableName,
                            List<BaseFilter> filters) {
        SqlParameter sqlParameter = parseSelect(tableName, AGGREGATE, filters, null, null);

        Executor executor = getExecutor();
        Value aggregate = executor.getOne(sqlParameter, Long.class);
        Long result = aggregate != null ? aggregate.getLong() : null;

        writeLogSelectCount(tableName, filters, result);
        return result;
    }

    /**
     * SELECT ONE
     *
     * @param tableName FROM table
     * @param keyValue  Key Value
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @return [ Column Name : Column Value ]
     */
    public Map<String, Value> selectOne(@NotNull String tableName,
                                        @NotNull Value keyValue, @Nullable String keyName, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);

        List<BaseFilter> filters = KeyParser.parseList(tableSchema, keyValue, keyName);
        Map<String, Value> result = selectOne(tableSchema, filters, null);

        writeLogSelectOne(tableName, keyValue, keyName, tableNum, result);
        return result;
    }

    /**
     * SELECT ONE
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param orders    [ the {@link Order} instance ]
     * @param tableNum  Sharding Table Num
     * @return [ Column Name : Column Value ]
     */
    public Map<String, Value> selectOne(@NotNull String tableName,
                                        List<BaseFilter> filters, List<Order> orders, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);
        Map<String, Value> result = selectOne(tableSchema, filters, orders);

        writeLogSelectOne(tableName, filters, orders, tableNum, result);
        return result;
    }

    /**
     * SELECT ONE
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param filters     [ the {@link BaseFilter} instance ]
     * @param orders      [ the {@link Order} instance ]
     * @return [ Column Name : Column Value ]
     */
    public Map<String, Value> selectOne(@NotNull TableSchema tableSchema,
                                        List<BaseFilter> filters, List<Order> orders) {
        Page page = new Page();
        page.setLimit(1);

        SqlParameter sqlParameter = parseSelect(tableSchema, filters, orders, page);

        List<String> columnNames = tableSchema.getColumnNames();
        Map<String, Class<?>> valueTypes = tableSchema.getValueTypes();

        Executor executor = getExecutor();
        Map<String, Value> result = executor.selectOne(sqlParameter, columnNames, valueTypes);

        writeLogSelectOne(tableSchema, filters, orders, result);
        return result;
    }

    /**
     * CHECK EXIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @return EXIST ? true : false
     */
    public boolean checkExist(@NotNull String tableName,
                              List<BaseFilter> filters, @Nullable Integer tableNum) {
        String joinedTableName = joinTableName(tableName, tableNum);
        Integer exist = checkExist(joinedTableName, filters);
        boolean result = exist != null && exist == 1;

        writeLogCheckExist(tableName, filters, tableNum, result);
        return result;
    }

    /**
     * CHECK EXIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @return EXIST ? 1 : null
     */
    public Integer checkExist(@NotNull String tableName,
                              List<BaseFilter> filters) {
        Page page = new Page();
        page.setLimit(1);

        SqlParameter sqlParameter = parseSelect(tableName, "1", filters, null, page);

        Executor executor = getExecutor();
        Value exist = executor.getOne(sqlParameter, Integer.class);
        Integer result = exist != null ? exist.getInteger() : null;

        writeLogCheckExist(tableName, filters, result);
        return result;
    }

    /**
     * INSERT
     *
     * @param tableName INSERT INTO table
     * @param values    [ Column Name : Column Value ]
     * @param tableNum  Sharding Table Num
     * @return AFFECTED ROWS
     */
    public int insert(@NotNull String tableName,
                      @NotNull Map<String, Value> values, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);
        int result = insert(tableSchema, values);

        writeLogInsert(tableName, values, tableNum, result);
        return result;
    }

    /**
     * INSERT
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param values      [ Column Name : Column Value ]
     * @return AFFECTED ROWS
     */
    public int insert(@NotNull TableSchema tableSchema,
                      @NotNull Map<String, Value> values) {
        SqlParameter sqlParameter = new SqlParameter();

        String sql = parseInsert(tableSchema, 1);
        sqlParameter.setSql(sql);

        List<String> parameterNames = tableSchema.getColumnsOnInsert();
        sqlParameter.setParameterNames(parameterNames);

        Map<String, Value> parameters = DefaultValues.setDefaultValuesOnInsert(tableSchema, values);
        sqlParameter.setParameters(parameters);

        Executor executor = getExecutor();
        int result = executor.update(sqlParameter);

        writeLogInsert(tableSchema, values, result);
        return result;
    }

    /**
     * BATCH INSERT
     *
     * @param tableName INSERT INTO table
     * @param list      [ [ Column Name : Column Value ] ]
     * @param tableNum  Sharding Table Num
     * @return AFFECTED ROWS
     */
    public int batchInsert(@NotNull String tableName,
                           @NotNull List<Map<String, Value>> list, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);
        int result = batchInsert(tableSchema, list);

        writeLogBatchInsert(tableName, list, tableNum, result);
        return result;
    }

    /**
     * BATCH INSERT
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param list        [ [ Column Name : Column Value ] ]
     * @return AFFECTED ROWS
     */
    public int batchInsert(@NotNull TableSchema tableSchema,
                           @NotNull List<Map<String, Value>> list) {
        List<Map<String, Value>> parametersList = DefaultValues.setDefaultValuesOnInsert(tableSchema, list);
        int batchSize = parametersList.size();
        if (batchSize == 0) {
            return 0;
        }

        SqlParameter sqlParameter = new SqlParameter();

        String sql = parseInsert(tableSchema, batchSize);
        sqlParameter.setSql(sql);

        List<String> parameterNames = tableSchema.getColumnsOnInsert();
        sqlParameter.setParameterNames(parameterNames);

        sqlParameter.setParametersList(parametersList);

        Executor executor = getExecutor();
        int result = executor.updateList(sqlParameter);

        writeLogBatchInsert(tableSchema, list, result);
        return result;
    }

    /**
     * UPDATE
     *
     * @param tableName UPDATE table
     * @param keyValue  Key Value
     * @param sets      [ column = column + 1 ]
     * @param setValues [ Set Name : Set Value ]
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @return AFFECTED ROWS
     */
    public int update(@NotNull String tableName,
                      @NotNull Value keyValue, List<String> sets, Map<String, Value> setValues, @Nullable String keyName, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);

        List<BaseFilter> filters = KeyParser.parseList(tableSchema, keyValue, keyName);
        int result = update(tableName, sets, setValues, filters, tableNum);

        writeLogUpdate(tableName, keyValue, sets, setValues, keyName, tableNum, result);
        return result;
    }

    /**
     * UPDATE
     *
     * @param tableName UPDATE table
     * @param sets      [ column = column + 1 ]
     * @param setValues [ Set Name : Set Value ]
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @return AFFECTED ROWS
     */
    public int update(@NotNull String tableName,
                      List<String> sets, Map<String, Value> setValues, List<BaseFilter> filters, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);

        List<String> setNames = SetParser.parseSetNames(tableSchema, setValues);
        int result = update(tableSchema.getTableName(), sets, setNames, setValues, filters);

        writeLogUpdate(tableName, sets, setValues, filters, tableNum, result);
        return result;
    }

    /**
     * UPDATE
     *
     * @param tableName UPDATE table
     * @param sets      [ column = column + 1 ]
     * @param setNames  [ Set Name ]
     * @param setValues [ Set Name : Set Value ]
     * @param filters   [ the {@link BaseFilter} instance ]
     * @return AFFECTED ROWS
     */
    public int update(@NotNull String tableName,
                      List<String> sets, List<String> setNames, Map<String, Value> setValues, List<BaseFilter> filters) {
        SqlParser sqlParser = getSqlParser();
        String joinedSets = SetParser.joinSets(sets, setNames);
        SqlParameter sqlParameter = sqlParser.parseUpdate(tableName, joinedSets, filters);

        List<String> parameterNames = PlaceholderValues.concatNames(setNames, sqlParameter.getParameterNames());
        Map<String, Value> parameters = PlaceholderValues.concatParameters(setValues, sqlParameter.getParameters());

        sqlParameter.setParameterNames(parameterNames);
        sqlParameter.setParameters(parameters);

        Executor executor = getExecutor();
        int result = executor.update(sqlParameter);

        writeLogUpdate(tableName, sets, setNames, setValues, filters, result);
        return result;
    }

    /**
     * BATCH UPDATE, No Transactional
     *
     * @param tableName UPDATE table
     * @param sets      [ column = column + 1 ]
     * @param setNames  [ Set Name ]
     * @param list      [ [ Parameter Name : Parameter Value ] ]
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @return [ AFFECTED ROWS ]
     */
    public int[] batchUpdate(@NotNull String tableName,
                             List<String> sets, List<String> setNames, List<Map<String, Value>> list, @Nullable String keyName, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);
        int[] result = batchUpdate(tableSchema, sets, setNames, list, keyName);

        writeLogBatchUpdate(tableName, sets, setNames, list, keyName, tableNum, result);
        return result;
    }

    /**
     * BATCH UPDATE, No Transactional
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param sets        [ column = column + 1 ]
     * @param setNames    [ Set Name ]
     * @param list        [ [ Parameter Name : Parameter Value ] ]
     * @param keyName     Key Name, if null ? Primary Key
     * @return [ AFFECTED ROWS ]
     */
    public int[] batchUpdate(@NotNull TableSchema tableSchema,
                             List<String> sets, List<String> setNames, List<Map<String, Value>> list, @Nullable String keyName) {
        String tableName = tableSchema.getTableName();
        String idName = keyName != null ? keyName.trim() : tableSchema.getIdName();
        Preconditions.requireNonEmpty(idName, "idName must not be empty, tableName: " + tableName);

        if (setNames != null) {
            setNames.remove(idName);
        }

        List<String> parameterNames = setNames != null ? new ArrayList<>(setNames) : new ArrayList<>();
        parameterNames.add(idName);

        SqlParser sqlParser = getSqlParser();
        String joinedSets = SetParser.joinSets(sets, setNames);
        List<BaseFilter> filters = KeyParser.parseList(idName);
        SqlParameter sqlParameter = sqlParser.parseUpdate(tableName, joinedSets, filters);

        sqlParameter.setParameterNames(parameterNames);
        sqlParameter.setParametersList(list);

        Executor executor = getExecutor();
        int[] result = executor.batchUpdate(sqlParameter);

        writeLogBatchUpdate(tableSchema, sets, setNames, list, keyName, result);
        return result;
    }

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param keyValue  Key Value
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @return AFFECTED ROWS
     */
    public int delete(@NotNull String tableName,
                      @NotNull Value keyValue, @Nullable String keyName, @Nullable Integer tableNum) {
        TableSchema tableSchema = getTableSchema(tableName, tableNum);

        List<BaseFilter> filters = KeyParser.parseList(tableSchema, keyValue, keyName);
        int result = delete(tableName, filters, tableNum);

        writeLogDelete(tableName, keyValue, keyName, tableNum, result);
        return result;
    }

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @return AFFECTED ROWS
     */
    public int delete(@NotNull String tableName,
                      List<BaseFilter> filters, @Nullable Integer tableNum) {
        String joinedTableName = joinTableName(tableName, tableNum);
        int result = delete(joinedTableName, filters);

        writeLogDelete(tableName, filters, tableNum, result);
        return result;
    }

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @return AFFECTED ROWS
     */
    public int delete(@NotNull String tableName,
                      List<BaseFilter> filters) {
        SqlParser sqlParser = getSqlParser();
        SqlParameter sqlParameter = sqlParser.parseDelete(tableName, filters);

        Executor executor = getExecutor();
        int result = executor.update(sqlParameter);

        writeLogDelete(tableName, filters, result);
        return result;
    }

    /**
     * SELECT column, column FROM table WHERE column = ? ORDER BY name ASC LIMIT offset, limit
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param filters     [ the {@link BaseFilter} instance ]
     * @param orders      [ the {@link Order} instance ]
     * @param page        the {@link Page} instance
     * @return the {@link SqlParameter} instance
     */
    @NotNull
    public SqlParameter parseSelect(@NotNull TableSchema tableSchema,
                                    List<BaseFilter> filters, List<Order> orders, Page page) {
        String tableName = tableSchema.getTableName();
        String joinedColumns = tableSchema.getJoinedColumnsOnSelect();
        return parseSelect(tableName, joinedColumns, filters, orders, page);
    }

    /**
     * SELECT column, column FROM table WHERE column = ? ORDER BY name ASC LIMIT offset, limit
     *
     * @param tableName FROM table
     * @param columns   column, COUNT(*) AS aggregate, 1
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param orders    [ the {@link Order} instance ]
     * @param page      the {@link Page} instance
     * @return the {@link SqlParameter} instance
     */
    @NotNull
    public SqlParameter parseSelect(@NotNull String tableName,
                                    @NotNull String columns, List<BaseFilter> filters, List<Order> orders, Page page) {
        SqlParser sqlParser = getSqlParser();
        return sqlParser.parseSelect(tableName, false, columns, filters, orders, page);
    }

    /**
     * INSERT INTO table (column, column) VALUES (?, ?), (?, ?), (?, ?)
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param batchSize   Batch Size
     * @return Parsed SQL
     */
    @NotNull
    public String parseInsert(@NotNull TableSchema tableSchema, int batchSize) {
        String tableName = tableSchema.getTableName();
        String joinedColumns = tableSchema.getJoinedColumnsOnInsert();
        String joinedValues = tableSchema.getJoinedValuesOnInsert();

        SqlParser sqlParser = getSqlParser();
        return sqlParser.parseInsert(tableName, joinedColumns, joinedValues, batchSize);
    }

    /**
     * TABLE NAME to TABLE SCHEMA
     *
     * @param tableName Table Name
     * @param tableNum  Sharding Table Num
     * @return the {@link TableSchema} instance
     */
    @NotNull
    public TableSchema getTableSchema(@NotNull String tableName, @Nullable Integer tableNum) {
        String joinedTableName = joinTableName(tableName, tableNum);

        TableSchema tableSchema = TableSchemaRegistry.get(joinedTableName);
        Preconditions.requireNonNull(tableSchema, "tableSchema must not be null, tableName: " + joinedTableName);

        return tableSchema;
    }

    /**
     * Join Table Name And Sharding Table Num
     *
     * @param tableName Table Name
     * @param tableNum  Sharding Table Num
     * @return the {@link TableSchema} instance
     */
    @NotNull
    public String joinTableName(@NotNull String tableName, @Nullable Integer tableNum) {
        if (tableNum == null) {
            return tableName;
        } else {
            return tableName + "_" + tableNum;
        }
    }

    @NotNull
    public Executor getExecutor() {
        return executor;
    }

    @NotNull
    public SqlParser getSqlParser() {
        return sqlParser;
    }

    /**
     * SELECT LIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param orders    [ the {@link Order} instance ]
     * @param page      the {@link Page} instance
     * @param tableNum  Sharding Table Num
     * @param result    [ [ Column Name : Column Value ] ]
     */
    protected void writeLogSelectList(String tableName,
                                      List<BaseFilter> filters, List<Order> orders, Page page, Integer tableNum, List<Map<String, Value>> result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.selectList(tableName, filters, orders, page, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * SELECT LIST
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param filters     [ the {@link BaseFilter} instance ]
     * @param orders      [ the {@link Order} instance ]
     * @param page        the {@link Page} instance
     * @param result      [ [ Column Name : Column Value ] ]
     */
    protected void writeLogSelectList(TableSchema tableSchema,
                                      List<BaseFilter> filters, List<Order> orders, Page page, List<Map<String, Value>> result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.selectList(tableSchema, filters, orders, page, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * SELECT COUNT
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    AGGREGATE
     */
    protected void writeLogSelectCount(String tableName,
                                       List<BaseFilter> filters, Integer tableNum, long result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.selectCount(tableName, filters, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * SELECT COUNT
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param result    AGGREGATE
     */
    protected void writeLogSelectCount(String tableName,
                                       List<BaseFilter> filters, Long result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.selectCount(tableName, filters, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * SELECT ONE
     *
     * @param tableName FROM table
     * @param keyValue  Key Value
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @param result    [ Column Name : Column Value ]
     */
    protected void writeLogSelectOne(String tableName,
                                     Value keyValue, String keyName, Integer tableNum, Map<String, Value> result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.selectOne(tableName, keyValue, keyName, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * SELECT ONE
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param orders    [ the {@link Order} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    [ Column Name : Column Value ]
     */
    protected void writeLogSelectOne(String tableName,
                                     List<BaseFilter> filters, List<Order> orders, Integer tableNum, Map<String, Value> result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.selectOne(tableName, filters, orders, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * SELECT ONE
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param filters     [ the {@link BaseFilter} instance ]
     * @param orders      [ the {@link Order} instance ]
     * @param result      [ Column Name : Column Value ]
     */
    protected void writeLogSelectOne(TableSchema tableSchema,
                                     List<BaseFilter> filters, List<Order> orders, Map<String, Value> result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.selectOne(tableSchema, filters, orders, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * CHECK EXIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    EXIST ? true : false
     */
    protected void writeLogCheckExist(String tableName,
                                      List<BaseFilter> filters, Integer tableNum, boolean result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.checkExist(tableName, filters, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * CHECK EXIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param result    EXIST ? 1 : null
     */
    protected void writeLogCheckExist(String tableName,
                                      List<BaseFilter> filters, Integer result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.checkExist(tableName, filters, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * INSERT
     *
     * @param tableName INSERT INTO table
     * @param values    [ Column Name : Column Value ]
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    protected void writeLogInsert(String tableName,
                                  Map<String, Value> values, Integer tableNum, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.insert(tableName, values, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * INSERT
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param values      [ Column Name : Column Value ]
     * @param result      AFFECTED ROWS
     */
    protected void writeLogInsert(TableSchema tableSchema,
                                  Map<String, Value> values, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.insert(tableSchema, values, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * BATCH INSERT
     *
     * @param tableName INSERT INTO table
     * @param list      [ [ Column Name : Column Value ] ]
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    protected void writeLogBatchInsert(String tableName,
                                       List<Map<String, Value>> list, Integer tableNum, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.batchInsert(tableName, list, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * BATCH INSERT
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param list        [ [ Column Name : Column Value ] ]
     * @param result      AFFECTED ROWS
     */
    protected void writeLogBatchInsert(TableSchema tableSchema,
                                       List<Map<String, Value>> list, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.batchInsert(tableSchema, list, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * UPDATE
     *
     * @param tableName UPDATE table
     * @param keyValue  Key Value
     * @param sets      [ column = column + 1 ]
     * @param setValues [ Set Name : Set Value ]
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    protected void writeLogUpdate(String tableName,
                                  Value keyValue, List<String> sets, Map<String, Value> setValues, String keyName, Integer tableNum, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.update(tableName, keyValue, sets, setValues, keyName, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * UPDATE
     *
     * @param tableName UPDATE table
     * @param sets      [ column = column + 1 ]
     * @param setValues [ Set Name : Set Value ]
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    protected void writeLogUpdate(String tableName,
                                  List<String> sets, Map<String, Value> setValues, List<BaseFilter> filters, Integer tableNum, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.update(tableName, sets, setValues, filters, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * UPDATE
     *
     * @param tableName UPDATE table
     * @param sets      [ column = column + 1 ]
     * @param setNames  [ Set Name ]
     * @param setValues [ Set Name : Set Value ]
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param result    AFFECTED ROWS
     */
    protected void writeLogUpdate(String tableName,
                                  List<String> sets, List<String> setNames, Map<String, Value> setValues, List<BaseFilter> filters, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.update(tableName, sets, setNames, setValues, filters, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * BATCH UPDATE, No Transactional
     *
     * @param tableName UPDATE table
     * @param sets      [ column = column + 1 ]
     * @param setNames  [ Set Name ]
     * @param list      [ [ Parameter Name : Parameter Value ] ]
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @param result    [ AFFECTED ROWS ]
     */
    protected void writeLogBatchUpdate(String tableName,
                                       List<String> sets, List<String> setNames, List<Map<String, Value>> list, String keyName, Integer tableNum, int[] result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.batchUpdate(tableName, sets, setNames, list, keyName, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * BATCH UPDATE, No Transactional
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param sets        [ column = column + 1 ]
     * @param setNames    [ Set Name ]
     * @param list        [ [ Parameter Name : Parameter Value ] ]
     * @param keyName     Key Name, if null ? Primary Key
     * @param result      [ AFFECTED ROWS ]
     */
    protected void writeLogBatchUpdate(TableSchema tableSchema,
                                       List<String> sets, List<String> setNames, List<Map<String, Value>> list, String keyName, int[] result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.batchUpdate(tableSchema, sets, setNames, list, keyName, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param keyValue  Key Value
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    protected void writeLogDelete(String tableName,
                                  Value keyValue, String keyName, Integer tableNum, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.delete(tableName, keyValue, keyName, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    protected void writeLogDelete(String tableName,
                                  List<BaseFilter> filters, Integer tableNum, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.delete(tableName, filters, tableNum, result);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param result    AFFECTED ROWS
     */
    protected void writeLogDelete(String tableName,
                                  List<BaseFilter> filters, int result) {
        try {
            TemplateLog logWriter = getLogWriter();
            if (logWriter != null) {
                logWriter.delete(tableName, filters, result);
            }
        } catch (Throwable ignored) {
        }
    }

    @Nullable
    public TemplateLog getLogWriter() {
        return logWriter;
    }

    public void setLogWriter(@Nullable TemplateLog logWriter) {
        this.logWriter = logWriter;
    }

}
