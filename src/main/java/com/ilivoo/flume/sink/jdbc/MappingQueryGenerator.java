package com.ilivoo.flume.sink.jdbc;

import com.ilivoo.flume.jdbc.DBContext;
import com.ilivoo.flume.jdbc.JDBCTable;
import com.ilivoo.flume.utils.JsonUtil;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.jooq.ConnectionRunnable;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.InsertOnDuplicateSetStep;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.conf.ParamType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MappingQueryGenerator implements QueryGenerator {

    private static final Logger log = LoggerFactory.getLogger(MappingQueryGenerator.class);

    private static final String TABLE = "table";

    private DBContext<JDBCTable> dbContext;

    private String dataFormat;

    private CounterGroup counterGroup;

    public MappingQueryGenerator(DBContext<JDBCTable> dbContext,
                                 String dataFormat,
                                 CounterGroup counterGroup) {
        this.dbContext = dbContext;
        this.dataFormat = dataFormat;
        this.counterGroup = counterGroup;
    }

    private void executeTableQuery(final DSLContext context, JDBCTable table, final List<Event> events) throws Exception {
        int mappedEvents = 0;
        for (Event event : events) {
            Map<Field, Object> fieldValues = new HashMap<>();
            Map<String, ?> values = event.getHeaders();
            if (dataFormat.equals(JDBCSink.DATA_FORMAT_BODY_JSON)) {
                values = JsonUtil.jsonToObjectMap(new String(event.getBody(), "UTF-8"));
            }
            for (Map.Entry<String, ?> entry : values.entrySet()) {
                String columnAlias = entry.getKey();
                if (columnAlias.equals(TABLE)) {
                    continue;
                }
                String columnName = table.getAliasColumn(columnAlias);
                if (!table.isColumnAccess(columnName)) {
                    continue;
                }
                Field field = null;
                for (Field f : table.getTable().fields()) {
                    if (f.getName().equalsIgnoreCase(columnName)) {
                        field = f;
                        break;
                    }
                }
                if (field == null) {
                    log.trace("Ignoring field: {}", columnAlias);
                    continue;
                }
                DataType dataType = field.getDataType();
                fieldValues.put(field, dataType.convert(entry.getValue()));
            }
            if (fieldValues.isEmpty()) {
                log.debug("Ignoring event, no mapped fields.");
            } else {
                mappedEvents++;
                final InsertSetStep insert = context.insertInto(table.getTable());
                if (insert instanceof InsertSetMoreStep) {
                    ((InsertSetMoreStep) insert).newRecord();
                }
                for (Map.Entry<Field, Object> entry : fieldValues.entrySet()) {
                    ((InsertSetMoreStep) insert).set(entry.getKey(), entry.getValue());
                }
                if (insert instanceof InsertSetMoreStep) {
                    InsertOnDuplicateSetStep step = ((InsertSetMoreStep) insert).onDuplicateKeyUpdate();
                    for (Map.Entry<Field, Object> entry : fieldValues.entrySet()) {
                        step.set(entry.getKey(), entry.getValue());
                    }
                }
                if (insert instanceof  InsertSetMoreStep) {
                    context.connection(new ConnectionRunnable() {
                        @Override
                        public void run(Connection connection) throws Exception {
                            String sql = ((InsertSetMoreStep) insert).getSQL(ParamType.INLINED);
                            connection.prepareStatement(sql).execute();
                        }
                    });
                }
            }
        }
        if (mappedEvents > 0) {
            counterGroup.addAndGet(table.getName(), new Long(mappedEvents));
            if (events.size() != mappedEvents) {
                log.warn("Event size {}, Inserted {}.", events.size(), mappedEvents);
            }
        } else {
            log.debug("No insert.");
        }
    }

    @Override
    public boolean executeQuery(DSLContext context, final List<Event> events) throws Exception {
        Map<String, List<Event>> tableEventMap = new HashMap<>();
        for (Event event : events) {
            String tableName = event.getHeaders().get(TABLE);
            List<Event> tableEvents = tableEventMap.get(tableName);
            if (tableEvents == null) {
                tableEvents = new ArrayList<>();
                tableEventMap.put(tableName, tableEvents);
            }
            tableEvents.add(event);
        }
        for (Map.Entry<String, List<Event>> entry : tableEventMap.entrySet()) {
            String tableAlias = entry.getKey();
            JDBCTable table = dbContext.getTableWithAlias(tableAlias);
            if (table == null) {
                table = new JDBCTable();
                table.setName(tableAlias);
                dbContext.addAccessTable(table);
            }
            executeTableQuery(context, table, entry.getValue());
        }
        return true;
    }
}
