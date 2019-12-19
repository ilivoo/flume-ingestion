package com.ilivoo.flume.jdbc;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.jooq.Field;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class JDBCTable {
    protected static Logger log = LoggerFactory.getLogger(JDBCTable.class);
    protected DBContext<?> dbContext;
    /**
     * table name, always real table name
     * source read from named table, and send to channel as alias
     * sink receive alias from channel, and send to named db table
     */
    protected String name;
    //db table
    protected Table table;
    //table alias
    protected String alias;
    //table fields
    protected Set<String> tableFields = new HashSet<>();
    //access columns
    protected Set<String> accessColumnSet = new HashSet<>();
    /**
     * column name, always real column name
     * source read from named column, and send to channel as alias
     * sink receive alias from channel, and send to named table column
     */
    protected BiMap<String, String> columnAliasMap = HashBiMap.create();

    protected void initTable(DBContext<?> dbContext) {
        this.dbContext = dbContext;
        table = dbContext.getDBTable(name);
        for (Field field : table.fields()) {
            tableFields.add(field.getName());
        }
        for (String column : accessColumnSet) {
            if (!tableFields.contains(column)) {
                throw new JDBCException(name + " table has no column exists: " + column);
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        this.alias = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public BiMap<String, String> getColumnAliasMap() {
        return columnAliasMap;
    }

    public String getAliasColumn(String alias) {
        String column = columnAliasMap.inverse().get(alias);
        if (column == null) {
            column = alias;
        }
        return column;
    }

    public String getColumnAlias(String column) {
        String alias = columnAliasMap.get(column);
        if (alias == null) {
            alias = column;
        }
        return alias;
    }

    public Set<String> getAccessColumnSet() {
        return accessColumnSet;
    }

    public boolean isColumnAccess(String column) {
        boolean result = true;
        if (!accessColumnSet.isEmpty() && !accessColumnSet.contains(column)) {
            result = false;
        }
        return result;
    }

    public Table getTable() {
        return table;
    }
}
