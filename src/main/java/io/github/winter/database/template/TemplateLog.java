package io.github.winter.database.template;

import io.github.winter.boot.filter.BaseFilter;
import io.github.winter.boot.filter.Order;
import io.github.winter.boot.filter.Page;
import io.github.winter.boot.tuple.Value;
import io.github.winter.database.table.TableSchema;

import java.util.List;
import java.util.Map;

/**
 * Template Log
 *
 * @author changebooks@qq.com
 */
public interface TemplateLog {
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
    void selectList(String tableName,
                    List<BaseFilter> filters, List<Order> orders, Page page, Integer tableNum, List<Map<String, Value>> result);

    /**
     * SELECT LIST
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param filters     [ the {@link BaseFilter} instance ]
     * @param orders      [ the {@link Order} instance ]
     * @param page        the {@link Page} instance
     * @param result      [ [ Column Name : Column Value ] ]
     */
    void selectList(TableSchema tableSchema,
                    List<BaseFilter> filters, List<Order> orders, Page page, List<Map<String, Value>> result);

    /**
     * SELECT COUNT
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    AGGREGATE
     */
    void selectCount(String tableName,
                     List<BaseFilter> filters, Integer tableNum, long result);

    /**
     * SELECT COUNT
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param result    AGGREGATE
     */
    void selectCount(String tableName,
                     List<BaseFilter> filters, Long result);

    /**
     * SELECT ONE
     *
     * @param tableName FROM table
     * @param keyValue  Key Value
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @param result    [ Column Name : Column Value ]
     */
    void selectOne(String tableName,
                   Value keyValue, String keyName, Integer tableNum, Map<String, Value> result);

    /**
     * SELECT ONE
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param orders    [ the {@link Order} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    [ Column Name : Column Value ]
     */
    void selectOne(String tableName,
                   List<BaseFilter> filters, List<Order> orders, Integer tableNum, Map<String, Value> result);

    /**
     * SELECT ONE
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param filters     [ the {@link BaseFilter} instance ]
     * @param orders      [ the {@link Order} instance ]
     * @param result      [ Column Name : Column Value ]
     */
    void selectOne(TableSchema tableSchema,
                   List<BaseFilter> filters, List<Order> orders, Map<String, Value> result);

    /**
     * CHECK EXIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    EXIST ? true : false
     */
    void checkExist(String tableName,
                    List<BaseFilter> filters, Integer tableNum, boolean result);

    /**
     * CHECK EXIST
     *
     * @param tableName FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param result    EXIST ? 1 : null
     */
    void checkExist(String tableName,
                    List<BaseFilter> filters, Integer result);

    /**
     * INSERT
     *
     * @param tableName INSERT INTO table
     * @param values    [ Column Name : Column Value ]
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    void insert(String tableName,
                Map<String, Value> values, Integer tableNum, int result);

    /**
     * INSERT
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param values      [ Column Name : Column Value ]
     * @param result      AFFECTED ROWS
     */
    void insert(TableSchema tableSchema,
                Map<String, Value> values, int result);

    /**
     * BATCH INSERT
     *
     * @param tableName INSERT INTO table
     * @param list      [ [ Column Name : Column Value ] ]
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    void batchInsert(String tableName,
                     List<Map<String, Value>> list, Integer tableNum, int result);

    /**
     * BATCH INSERT
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param list        [ [ Column Name : Column Value ] ]
     * @param result      AFFECTED ROWS
     */
    void batchInsert(TableSchema tableSchema,
                     List<Map<String, Value>> list, int result);

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
    void update(String tableName,
                Value keyValue, List<String> sets, Map<String, Value> setValues, String keyName, Integer tableNum, int result);

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
    void update(String tableName,
                List<String> sets, Map<String, Value> setValues, List<BaseFilter> filters, Integer tableNum, int result);

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
    void update(String tableName,
                List<String> sets, List<String> setNames, Map<String, Value> setValues, List<BaseFilter> filters, int result);

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
    void batchUpdate(String tableName,
                     List<String> sets, List<String> setNames, List<Map<String, Value>> list, String keyName, Integer tableNum, int[] result);

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
    void batchUpdate(TableSchema tableSchema,
                     List<String> sets, List<String> setNames, List<Map<String, Value>> list, String keyName, int[] result);

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param keyValue  Key Value
     * @param keyName   Key Name, if null ? Primary Key
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    void delete(String tableName,
                Value keyValue, String keyName, Integer tableNum, int result);

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param tableNum  Sharding Table Num
     * @param result    AFFECTED ROWS
     */
    void delete(String tableName,
                List<BaseFilter> filters, Integer tableNum, int result);

    /**
     * DELETE
     *
     * @param tableName DELETE FROM table
     * @param filters   [ the {@link BaseFilter} instance ]
     * @param result    AFFECTED ROWS
     */
    void delete(String tableName,
                List<BaseFilter> filters, int result);

}
