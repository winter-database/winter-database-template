package io.github.winter.database.template.value;

import io.github.winter.boot.sql.Preconditions;
import io.github.winter.boot.tuple.Value;

import java.util.*;

/**
 * Placeholder Values
 *
 * @author changebooks@qq.com
 */
public final class PlaceholderValues {
    /**
     * Prepared Statement Setter Placeholder
     */
    private static final String PLACEHOLDER = " = ?";

    private PlaceholderValues() {
    }

    /**
     * Concat Names
     *
     * @param placeholderNames [ Placeholder Name ]
     * @param parameterNames   [ Parameter Name ]
     * @return [ Parameter Name ]
     */
    public static List<String> concatNames(List<String> placeholderNames, List<String> parameterNames) {
        if (placeholderNames == null) {
            return parameterNames;
        }

        List<String> result = new ArrayList<>();

        for (String placeholderName : placeholderNames) {
            Preconditions.requireNonNull(placeholderName, "placeholderName must not be null");
            result.add("[" + placeholderName + "]");
        }

        if (parameterNames != null) {
            result.addAll(parameterNames);
        }

        return result;
    }

    /**
     * Concat Parameters
     *
     * @param placeholderParameters [ Placeholder Name : Placeholder Value ]
     * @param parameters            [ Parameter Name : Parameter Value ]
     * @return [ Parameter Name : Parameter Value ]
     */
    public static Map<String, Value> concatParameters(Map<String, Value> placeholderParameters, Map<String, Value> parameters) {
        if (placeholderParameters == null) {
            return parameters;
        }

        Map<String, Value> result = new HashMap<>();

        for (Map.Entry<String, Value> entry : placeholderParameters.entrySet()) {
            if (entry == null) {
                continue;
            }

            String key = entry.getKey();
            if (key == null) {
                continue;
            }

            String name = "[" + key + "]";
            Value value = entry.getValue();

            result.put(name, value);
        }

        if (parameters != null) {
            result.putAll(parameters);
        }

        return result;
    }

    /**
     * Join Placeholder
     *
     * @param columnNames [ Column Name ]
     * @return [ column = ? ]
     */
    public static List<String> joinPlaceholder(List<String> columnNames) {
        if (columnNames != null) {
            return columnNames.stream()
                    .map(PlaceholderValues::joinPlaceholder)
                    .filter(Objects::nonNull)
                    .filter(x -> !x.isEmpty())
                    .toList();
        } else {
            return null;
        }
    }

    /**
     * Join Placeholder
     *
     * @param columnName Column Name
     * @return column = ?
     */
    public static String joinPlaceholder(String columnName) {
        if (columnName != null) {
            return columnName.isBlank() ? "" : columnName + PLACEHOLDER;
        } else {
            return null;
        }
    }

}
