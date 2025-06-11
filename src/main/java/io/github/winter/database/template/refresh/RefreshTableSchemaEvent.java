package io.github.winter.database.template.refresh;

import org.springframework.context.ApplicationEvent;

/**
 * 刷表概要
 *
 * @author changebooks@qq.com
 */
public class RefreshTableSchemaEvent extends ApplicationEvent {
    /**
     * Table Name = null ? Refresh All
     */
    private String tableName;

    public RefreshTableSchemaEvent() {
        super("-- Refresh All --");
    }

    public RefreshTableSchemaEvent(String tableName) {
        super(tableName);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
