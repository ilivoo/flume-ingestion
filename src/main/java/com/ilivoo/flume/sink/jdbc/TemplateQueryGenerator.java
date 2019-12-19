package com.ilivoo.flume.sink.jdbc;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.ilivoo.flume.jdbc.DBContext;
import com.ilivoo.flume.jdbc.JDBCTable;
import com.ilivoo.flume.utils.JsonUtil;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TemplateQueryGenerator implements QueryGenerator {

    private static final Logger log = LoggerFactory.getLogger(TemplateQueryGenerator.class);

    private static final Pattern TABLE_PATTERN = Pattern.compile("\\$\\$\\{(?<table>[^\\s.{}]+)\\}");

    private static final Pattern FIELD_PATTERN = Pattern.compile("\\$\\{(?<field>[^\\s.{}]+)");

    private static final Pattern VALUE_PATTERN = Pattern.compile("\\#\\{(?<part>[^\\s.{}]+)(?:\\.(?<header>[^\\s.{}]+))?\\}");

    private static final String BODY = "BODY";

    private static final String TABLE = "table";

    private final List<Parameter> parameters;

    private final String sql;

    private JDBCTable table;

    private String dataFormat;

    private CounterGroup counterGroup;

    public TemplateQueryGenerator(DBContext<JDBCTable> dbContext, String sql, String dataFormat, CounterGroup counterGroup) {
        this.dataFormat = dataFormat;
        this.counterGroup = counterGroup;
        //match table name
        Matcher tableMatcher = TABLE_PATTERN.matcher(sql);
        String tableName = null;
        while (tableMatcher.find()) {
            tableName = tableMatcher.group(TABLE);
        }
        sql = tableMatcher.replaceAll(tableName);

        //match field name
        Matcher fieldMatcher = FIELD_PATTERN.matcher(sql);
        List<String> fields = new ArrayList<>();
        while (fieldMatcher.find()) {
            String field = fieldMatcher.group("field");
            fields.add(field);
        }
        for (String name : fields) {
            sql = sql.replaceFirst("\\$\\{" + name + "}", name);
        }

        //match value name
        parameters = new ArrayList<>();
        Matcher valueMatcher = VALUE_PATTERN.matcher(sql);
        while (valueMatcher.find()) {
            String part = valueMatcher.group("part").toUpperCase(Locale.ENGLISH);
            String header = valueMatcher.group("header");
            if (BODY.equals(part) && header != null) {
                throw new JDBCSinkException("BODY parameter must have no header name specifier (${body}, not (${body.header}");
            }
            Parameter parameter = new Parameter(header);
            parameters.add(parameter);
        }
        sql = valueMatcher.replaceAll("?");
        this.sql = sql;

        //parse parameter
        if (fields.size() != parameters.size()) {
            throw new JDBCSinkException("fields size not equal values size");
        }
        this.table = new JDBCTable();
        this.table.setName(tableName);
        dbContext.addAccessTable(this.table);
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i);
            Parameter parameter = parameters.get(i);
            Field field = null;
            for (Field f : table.getTable().fields()) {
                if (f.getName().equalsIgnoreCase(fieldName)) {
                    field = f;
                    break;
                }
            }
            if (field == null) {
                throw new JDBCSinkException("no field find: " + fieldName + ", in table: " + tableName);
            }
            parameter.setDataType(field.getDataType());
        }
    }

    @Override
    public boolean executeQuery(DSLContext context, final List<Event> events) throws Exception {
        List<Query> queries = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            final Object[] bindings = new Object[this.parameters.size()];
            Map<String, ?> headerValue = events.get(i).getHeaders();
            if (dataFormat.equals(JDBCSink.DATA_FORMAT_BODY_JSON)) {
                String json = new String(events.get(i).getBody(), "UTF-8");
                headerValue = JsonUtil.jsonToObjectMap(json);
            }
            for (int j = 0; j < this.parameters.size(); j++) {
                bindings[j] = this.parameters.get(j).binding(events.get(i), headerValue);
            }
            queries.add(context.query(this.sql, bindings));
        }
        context.batch(queries).execute();
        counterGroup.addAndGet(table.getName(), new Long(events.size()));
        return true;
    }

    private static class Parameter {

        private final String header;
        private DataType<?> dataType;

        Parameter(final String header) {
            this.header = header;
        }

        void setDataType(DataType<?> dataType) {
            this.dataType = dataType;
        }

        Object binding(Event event, Map<String, ?> headerValues) {
            if (header == null) {
                final byte body[] = event.getBody();
                return dataType.convert(new String(body, Charsets.UTF_8));
            } else {
                for (final Map.Entry<String, ?> entry : headerValues.entrySet()) {
                    if (entry.getKey().equals(header)) {
                        return dataType.convert(entry.getValue());
                    }
                }
            }
            log.trace("No bindable field found for {}", this);
            return null;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(Parameter.class)
                    .add("header", header).add("dataType", dataType).toString();
        }
    }

}
