package com.ilivoo.flume.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ilivoo.flume.source.jdbc.JDBCSourceTable;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.flume.Context;
import org.jooq.ConnectionRunnable;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JDBCHelper {
    private static final Logger log = LoggerFactory.getLogger(JDBCHelper.class);
    //common
    public static final String SEPARATOR = ".";
    public static final String TABLES = "tables";
    public static final String TABLES_PREFIX = TABLES + SEPARATOR;
    public static final String DEFAULT_ACCESS_TABLE_AND_COLUMNS = "*";
    public static final String TABLE_COLUMNS = "columns";
    public static final String TABLE_COLUMNS_PREFIX = TABLE_COLUMNS + SEPARATOR;
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_READ_BATCH_SIZE = 100;
    public static final int DEFAULT_WRITE_BATCH_SIZE = 100;
    public static final String CONN = "conn.";
    public static final String CONN_PROVIDER = "provider";
    public static final String DEFAULT_CONN_PROVIDER = "com.ilivoo.flume.jdbc.HikariDataSourceProvider";
    //source
    public static final String TABLE_WHERE = "where";
    public static final String TABLE_INCREMENT = "increments";
    public static final String TABLE_INCREMENT_PREFIX = TABLE_INCREMENT + SEPARATOR;
    public static final String TABLE_INCREMENT_FIND_NEW = "findNew";
    public static final boolean DEFAULT_TABLE_INCREMENT_FIND_NEW = true;
    public static final String TABLE_INCREMENT_EXCLUDES = "excludes";
    public static final String TABLE_INCREMENT_INCLUDES = "includes";
    public static final String TABLE_INCREMENT_DEFAULT_START = "defaultStart";
    public static final String TABLE_INCREMENT_STARTS = "starts";
    public static final String TABLE_INCREMENT_STARTS_PREFIX = TABLE_INCREMENT_STARTS + SEPARATOR;
    public static final String TABLE_INCREMENT_STRICT = "strict";
    public static final boolean DEFAULT_TABLE_INCREMENT_STRICT = true;
    public static final String TABLE_COLUMN_CONVERT = "convert";
    public static final String TABLE_IDLE_MAX = "idleMax";
    public static final String TABLE_IDLE_INTERVAL = "idleInterval";
    public static final long DEFAULT_TABLE_IDLE_MAX = 60 * 60 * 1000;
    public static final long DEFAULT_TABLE_IDLE_INTERVAL = 60 * 1000;
    public static final String POSITION_DIR = "positionDir";
    public static final String DEFAULT_POSITION_DIR = "/.flume";


    public static <T extends JDBCTable> DBContext<T> create(Context context, Class<T> clz) {
        String providerClass = context.getString(CONN + CONN_PROVIDER, DEFAULT_CONN_PROVIDER);
        Properties connProps = new Properties();
        connProps.putAll(context.getSubProperties(CONN));
        connProps.remove(CONN_PROVIDER);
        DSLContext create;
        try {
            DataSourceProvider provider = (DataSourceProvider) Thread.currentThread().getContextClassLoader()
                    .loadClass(providerClass).newInstance();
            provider.configProps(connProps);
            create = DSL.using(provider.createDataSource(), JDBCUtils.dialect(provider.getUrl()));
            log.info("Server Type: " + create.dialect().name());
        } catch (Exception e) {
            throw new JDBCException("DSL context create error", e);
        }

        final MutableObject mutableObject = new MutableObject();
        create.connection(new ConnectionRunnable() {
            @Override
            public void run(Connection connection) throws Exception {
                mutableObject.setValue(connection.getCatalog());
            }
        });
        String catalog = mutableObject.getValue().toString();
        if (Strings.isNullOrEmpty(catalog)) {
            throw new JDBCException("database should specify");
        }
        String homePath = System.getProperty("user.home").replace('\\', '/');
        String positionPath = context.getString(POSITION_DIR, homePath + DEFAULT_POSITION_DIR);
        positionPath = positionPath + "/" + catalog;
        try {
            Files.createDirectories(Paths.get(positionPath));
        } catch (IOException e) {
            throw new JDBCException("Error creating positionFile parent directories", e);
        }
        DBContext<T> dbContext = new DBContext<>(create, catalog, parseTable(context, clz), positionPath);
        if (clz == JDBCTable.class) {
            Integer readBatchSize = context.getInteger(BATCH_SIZE, DEFAULT_READ_BATCH_SIZE);
            Preconditions.checkArgument(readBatchSize > 0);
            dbContext.setReadBatchSize(readBatchSize);
        } else if (clz == JDBCSourceTable.class) {
            Integer writeBatchSize = context.getInteger(BATCH_SIZE, DEFAULT_WRITE_BATCH_SIZE);
            Preconditions.checkArgument(writeBatchSize > 0);
            dbContext.setWriteBatchSize(writeBatchSize);
        }
        return dbContext;
    }

    private static <T extends JDBCTable> List<T> parseTable(Context context, Class<T> clz) {
        List<T> result = new ArrayList<>();
        String[] limitTables = new String[0];
        String tables = context.getString(TABLES);
        long idleMax = context.getLong(TABLE_IDLE_MAX, DEFAULT_TABLE_IDLE_MAX);
        long idleInterval = context.getLong(TABLE_IDLE_INTERVAL, DEFAULT_TABLE_IDLE_INTERVAL);
        if (!Strings.isNullOrEmpty(tables) && !tables.equals(DEFAULT_ACCESS_TABLE_AND_COLUMNS)) {
            limitTables = tables.split("\\s+");
        }
        for (String table : limitTables) {
            String tableKey = TABLES_PREFIX + table;
            String alias = context.getString(tableKey);
            if (Strings.isNullOrEmpty(alias)) {
                alias = table;
            }
            Context tableContext = new Context(context.getSubProperties(tableKey + "."));
            String columns = tableContext.getString(TABLE_COLUMNS);
            String[] accessColumns = new String[0];
            if (!Strings.isNullOrEmpty(columns) && !columns.equals(DEFAULT_ACCESS_TABLE_AND_COLUMNS)) {
                accessColumns = columns.split("\\s+");
            }
            Map<String, String> columnPropMap = tableContext.getSubProperties(TABLE_COLUMNS_PREFIX);
            Map<String, String> columnAliasMap = new HashMap<>();
            for (Map.Entry<String, String> kv : columnPropMap.entrySet()) {
                String key = kv.getKey();
                if (key.contains(SEPARATOR)) {
                    String column = key.substring(0, key.indexOf(SEPARATOR));
                    if (!columnAliasMap.containsKey(column)) {
                        columnAliasMap.put(column, column);
                    }
                } else {
                    columnAliasMap.put(key, kv.getValue());
                }
            }
            T t;
            try {
                t = clz.newInstance();
            } catch (ReflectiveOperationException e) {
                throw new JDBCException("reflective operation exception");
            }
            t.setName(table);
            t.setAlias(alias);
            t.getAccessColumnSet().addAll(Arrays.asList(accessColumns));
            t.getColumnAliasMap().putAll(columnAliasMap);
            if (clz == JDBCSourceTable.class) {
                JDBCSourceTable jst = (JDBCSourceTable) t;
                long tableIdleMax = tableContext.getLong(TABLE_IDLE_MAX, idleMax);
                jst.setIdleMax(tableIdleMax);
                long tableIdleInterval = tableContext.getLong(TABLE_IDLE_INTERVAL, idleInterval);
                jst.setIdleInterval(tableIdleInterval);
                String tableWhere = tableContext.getString(TABLE_WHERE);
                jst.setWhere(tableWhere);
                String incrementStr = tableContext.getString(TABLE_INCREMENT);
                String[] increments = new String[0];
                if (!Strings.isNullOrEmpty(incrementStr)) {
                    increments = incrementStr.split("\\s+");
                }
                Preconditions.checkArgument(increments.length >= 1 && increments.length <= 2, jst.getName() + " increments config error");
                jst.setIncrements(increments);
                Context incrementContext = new Context(tableContext.getSubProperties(TABLE_INCREMENT_PREFIX));
                //column convert
                for (Map.Entry<String, String> kv : columnAliasMap.entrySet()) {
                    String convertValue = columnPropMap.get(kv.getKey() + SEPARATOR + TABLE_COLUMN_CONVERT);
                    if (convertValue != null) {
                        jst.getColumnConvertMap().put(kv.getKey(), convertValue);
                    }
                }
                //include
                String includeStr = incrementContext.getString(TABLE_INCREMENT_INCLUDES);
                String[] includes = new String[0];
                if (!Strings.isNullOrEmpty(includeStr)) {
                    includes = includeStr.split("\\s+");
                }
                jst.getIncludes().addAll(Arrays.asList(includes));
                //exclude
                String excludeStr = incrementContext.getString(TABLE_INCREMENT_EXCLUDES);
                String[] excludes = new String[0];
                if (!Strings.isNullOrEmpty(excludeStr)) {
                    excludes = excludeStr.split("\\s+");
                }
                jst.getExcludes().addAll(Arrays.asList(excludes));
                boolean findNew = incrementContext.getBoolean(TABLE_INCREMENT_FIND_NEW, DEFAULT_TABLE_INCREMENT_FIND_NEW);
                jst.setFindNew(findNew);
                jst.setDefaultStart(incrementContext.getString(TABLE_INCREMENT_DEFAULT_START));
                Map<String, String> start = incrementContext.getSubProperties(TABLE_INCREMENT_STARTS_PREFIX);
                jst.getStart().putAll(start);
                jst.setStrict(incrementContext.getBoolean(TABLE_INCREMENT_STRICT, DEFAULT_TABLE_INCREMENT_STRICT));
                if (increments.length == 1) {
                    Preconditions.checkArgument(excludes.length == 0 && start.size() == 0);
                }
            }
            result.add(t);
        }
        return result;
    }
}
