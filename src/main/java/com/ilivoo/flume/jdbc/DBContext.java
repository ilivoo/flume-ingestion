package com.ilivoo.flume.jdbc;

import com.google.common.collect.Lists;
import org.jooq.DSLContext;
import org.jooq.Name;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBContext<T extends JDBCTable> {

    private static final Logger log = LoggerFactory.getLogger(DBContext.class);
    private final DSLContext dslContext;
    private String positionPath;
    private String catalog;

    private Map<String, Table> tableMap = new HashMap<>();

    private Map<String, T> jdbcTableMap = new HashMap<>();

    private Map<String, T> jdbcAliasTableMap = new HashMap<>();

    private Set<String> accessTableSet = new HashSet<>();

    private Set<String> accessAliasTableSet = new HashSet<>();

    private int readBatchSize;

    private int writeBatchSize;

    //限制表和表的列， 这样在sink和source中都可以使用到
    public DBContext(DSLContext dslContext, String catalog, List<T> accessTables, String positionPath) {
        this.dslContext = dslContext;
        this.catalog = catalog;
        this.positionPath = positionPath;
        for (T t : accessTables) {
            this.jdbcTableMap.put(t.getName(), t);
            this.jdbcAliasTableMap.put(t.getAlias(), t);
            this.accessTableSet.add(t.getName());
            this.accessAliasTableSet.add(t.getAlias());
        }
        if (accessTables.size() == 0) {
            log.warn("{} database no table limit", catalog);
        }
        for (T t : jdbcTableMap.values()) {
            t.initTable(this);
        }
    }

    public void addAccessTable(T table) {
        this.jdbcTableMap.put(table.getName(), table);
        this.jdbcAliasTableMap.put(table.getAlias(), table);
        table.initTable(this);
    }

    Table getDBTable(String tableName) {
        if (!accessTableSet.isEmpty() && !accessTableSet.contains(tableName)) {
            throw new JDBCException("table sink limit: " + tableName);
        }
        Table result = tableMap.get(tableName);
        if (result == null) {
            for (Table table : dslContext.meta().getTables()) {
                Name tableQN = table.getQualifiedName();
                if (tableQN.first().equalsIgnoreCase(catalog)
                        && tableQN.last().equalsIgnoreCase(tableName)) {
                    result = table;
                    tableMap.put(tableName, table);
                    break;
                }
            }
        }
        if (result == null) {
            throw new JDBCException(String.format("%s database no table %s find", catalog, tableName));
        }
        return result;
    }

    public T getTableWithAlias(String alias) {
        if (!accessAliasTableSet.isEmpty() && !accessAliasTableSet.contains(alias)) {
            throw new JDBCException("table sink limit: " + alias);
        }
        return jdbcAliasTableMap.get(alias);
    }

    public DSLContext dslContext() {
        return dslContext;
    }

    public List<T> getTables() {
        return Lists.newArrayList(jdbcTableMap.values());
    }

    public int getReadBatchSize() {
        return readBatchSize;
    }

    public void setReadBatchSize(int readBatchSize) {
        this.readBatchSize = readBatchSize;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    public String getPositionPath() {
        return positionPath;
    }

    public void setPositionPath(String positionPath) {
        this.positionPath = positionPath;
    }
}
