package io.github.winter.database.template.parser;

import io.github.winter.boot.tuple.Value;
import io.github.winter.database.table.TableSchema;
import io.github.winter.database.template.value.PlaceholderValues;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Set Parser
 *
 * @author changebooks@qq.com
 */
public final class SetParser {

    private SetParser() {
    }

    /**
     * Parse Set Names
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param setValues   [ Set Name : Set Value ]
     * @return [ Set Name ]
     */
    @NotNull
    public static List<String> parseSetNames(@NotNull TableSchema tableSchema, Map<String, Value> setValues) {
        List<String> result = new ArrayList<>();
        if (setValues == null) {
            return result;
        }

        Set<String> columnNames = tableSchema.getColumnsOnUpdate();
        for (Map.Entry<String, Value> entry : setValues.entrySet()) {
            if (entry == null) {
                continue;
            }

            String columnName = entry.getKey();
            if (columnName == null) {
                continue;
            }

            Value value = entry.getValue();
            if (value == null) {
                continue;
            }

            if (columnNames.contains(columnName)) {
                result.add(columnName);
            }
        }

        return result;
    }

    /**
     * Join Sets
     *
     * @param sets     [ column = column + 1 ]
     * @param setNames [ Set Name ]
     * @return column = column + 1, column = ?
     */
    @NotNull
    public static String joinSets(List<String> sets, List<String> setNames) {
        List<String> result = sets != null ? new ArrayList<>(sets) : new ArrayList<>();

        List<String> setPlaceholders = PlaceholderValues.joinPlaceholder(setNames);
        if (setPlaceholders != null) {
            result.addAll(setPlaceholders);
        }

        return String.join(", ", result);
    }

}
