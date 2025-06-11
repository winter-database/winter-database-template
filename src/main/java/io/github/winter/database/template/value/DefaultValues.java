package io.github.winter.database.template.value;

import io.github.winter.boot.sql.Preconditions;
import io.github.winter.boot.tuple.Value;
import io.github.winter.database.table.TableSchema;
import jakarta.validation.constraints.NotNull;

import java.util.*;

/**
 * Default Values
 *
 * @author changebooks@qq.com
 */
public final class DefaultValues {

    private DefaultValues() {
    }

    /**
     * Set Default Values On Insert
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param list        [ [ Parameter Name : Parameter Value ] ]
     * @return [ [ Parameter Name : Parameter Value ] ]
     */
    @NotNull
    public static List<Map<String, Value>> setDefaultValuesOnInsert(@NotNull TableSchema tableSchema, List<Map<String, Value>> list) {
        List<String> columnNames = tableSchema.getColumnsOnInsert();
        Map<String, Value> defaultValues = tableSchema.getDefaultValues();
        return setDefaultValues(columnNames, defaultValues, list);
    }

    /**
     * Set Default Values
     *
     * @param columnNames   [ Column Name ]
     * @param defaultValues [ Column Name : Default Value ]
     * @param list          [ [ Parameter Name : Parameter Value ] ]
     * @return [ [ Parameter Name : Parameter Value ] ]
     */
    @NotNull
    public static List<Map<String, Value>> setDefaultValues(@NotNull List<String> columnNames,
                                                            Map<String, Value> defaultValues, List<Map<String, Value>> list) {
        return Optional.ofNullable(list)
                .orElse(Collections.emptyList())
                .stream()
                .map(values -> setDefaultValues(columnNames, defaultValues, values))
                .toList();
    }

    /**
     * Set Default Values
     *
     * @param columnNames   [ Column Name ]
     * @param defaultValues [ Column Name : Default Value ]
     * @param values        [ Parameter Name : Parameter Value ]
     * @return [ Parameter Name : Parameter Value ]
     */
    @NotNull
    public static Map<String, Value> setDefaultValues(@NotNull List<String> columnNames,
                                                      Map<String, Value> defaultValues, Map<String, Value> values) {
        Map<String, Value> result = new HashMap<>();

        for (String columnName : columnNames) {
            Preconditions.requireNonNull(columnName, "columnName must not be null");

            Value value = values != null ? values.get(columnName) : null;
            if (value != null) {
                result.put(columnName, value);
                continue;
            }

            Value defaultValue = defaultValues != null ? defaultValues.get(columnName) : null;
            Preconditions.requireNonNull(defaultValue, "defaultValue must not be null, columnName: " + columnName);

            result.put(columnName, defaultValue);
        }

        return result;
    }

    /**
     * Set Default Values On Insert
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param values      [ Parameter Name : Parameter Value ]
     * @return [ Parameter Name : Parameter Value ]
     */
    @NotNull
    public static Map<String, Value> setDefaultValuesOnInsert(@NotNull TableSchema tableSchema, Map<String, Value> values) {
        List<String> columnNames = tableSchema.getColumnsOnInsert();
        Map<String, Value> defaultValues = tableSchema.getDefaultValues();
        return setDefaultValues(columnNames, defaultValues, values);
    }

}
