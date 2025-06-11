package io.github.winter.database.template.parser;

import io.github.winter.boot.filter.BaseFilter;
import io.github.winter.boot.filter.ExpressionCode;
import io.github.winter.boot.filter.ExpressionFilter;
import io.github.winter.boot.filter.Parameter;
import io.github.winter.boot.sql.Preconditions;
import io.github.winter.boot.tuple.Value;
import io.github.winter.database.table.TableSchema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

import java.util.Collections;
import java.util.List;

/**
 * Key Parser
 *
 * @author changebooks@qq.com
 */
public final class KeyParser {

    private KeyParser() {
    }

    /**
     * Key = :Key
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param keyValue    Key Value
     * @param keyName     Key Name
     * @return [ the {@link BaseFilter} instance ]
     */
    @NotNull
    public static List<BaseFilter> parseList(@NotNull TableSchema tableSchema, @NotNull Value keyValue, @Nullable String keyName) {
        ExpressionFilter filter = parse(tableSchema, keyValue, keyName);
        return Collections.singletonList(filter);
    }

    /**
     * Key = :Key
     *
     * @param keyName  Key Name
     * @param keyValue Key Value
     * @return [ the {@link BaseFilter} instance ]
     */
    @NotNull
    public static List<BaseFilter> parseList(@NotNull String keyName, @NotNull Value keyValue) {
        ExpressionFilter filter = parse(keyName, keyValue);
        return Collections.singletonList(filter);
    }

    /**
     * Key = :Key
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param keyValue    Key Value
     * @param keyName     Key Name
     * @return the {@link ExpressionFilter} instance
     */
    @NotNull
    public static ExpressionFilter parse(@NotNull TableSchema tableSchema, @NotNull Value keyValue, @Nullable String keyName) {
        String name = keyName != null ? keyName.trim() : tableSchema.getIdName();
        return parse(name, keyValue);
    }

    /**
     * Key = :Key
     *
     * @param keyName  Key Name
     * @param keyValue Key Value
     * @return the {@link ExpressionFilter} instance
     */
    @NotNull
    public static ExpressionFilter parse(@NotNull String keyName, @NotNull Value keyValue) {
        Parameter parameter = new Parameter();
        parameter.setName(keyName);
        parameter.setValue(keyValue);

        ExpressionFilter filter = parse(keyName);
        filter.setParameter(parameter);

        return filter;
    }

    /**
     * Key = ?
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param keyName     Key Name
     * @return [ the {@link BaseFilter} instance ]
     */
    @NotNull
    public static List<BaseFilter> parseList(@NotNull TableSchema tableSchema, @Nullable String keyName) {
        ExpressionFilter filter = parse(tableSchema, keyName);
        return Collections.singletonList(filter);
    }

    /**
     * Key = ?
     *
     * @param keyName Key Name
     * @return [ the {@link BaseFilter} instance ]
     */
    @NotNull
    public static List<BaseFilter> parseList(@NotNull String keyName) {
        ExpressionFilter filter = parse(keyName);
        return Collections.singletonList(filter);
    }

    /**
     * Key = ?
     *
     * @param tableSchema the {@link TableSchema} instance
     * @param keyName     Key Name
     * @return the {@link ExpressionFilter} instance
     */
    @NotNull
    public static ExpressionFilter parse(@NotNull TableSchema tableSchema, @Nullable String keyName) {
        String name = keyName != null ? keyName.trim() : tableSchema.getIdName();
        return parse(name);
    }

    /**
     * Key = ?
     *
     * @param keyName Key Name
     * @return the {@link ExpressionFilter} instance
     */
    @NotNull
    public static ExpressionFilter parse(@NotNull String keyName) {
        Preconditions.requireNonEmpty(keyName, "keyName must not be empty");

        ExpressionFilter filter = new ExpressionFilter();
        filter.setName(keyName);
        filter.setCode(ExpressionCode.EQ);

        return filter;
    }

}
