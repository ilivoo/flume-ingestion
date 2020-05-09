package com.ilivoo.flume.source.jdbc;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ilivoo.flume.jdbc.DBContext;
import com.ilivoo.flume.jdbc.JDBCTable;
import com.ilivoo.flume.sink.jdbc.JDBCSinkException;
import com.ilivoo.flume.utils.JsonUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.jooq.Condition;
import org.jooq.ConnectionRunnable;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Select;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.min;
import static org.jooq.impl.DSL.trueCondition;

public class JDBCSourceTable extends JDBCTable {
    //increment columns, identify:increment
    private String[] increments;
    private String defaultStart;
    private IdentifyIdle tableIdle;
    //include increment, identify
    private Set<String> includes = new HashSet<>();
    //excludes increment, identify
    private Set<String> excludes = new HashSet<>();
    //increment start, identify:incrementValue
    private Map<String, String> start = new ConcurrentHashMap<>();
    //idle identify, identify:IdentifyIdle
    private Map<String, IdentifyIdle> idleMap = new HashMap<>();
    //column convert
    private Map<String, String> columnConvertMap = new HashMap<>();
    //increment find new identify
    private boolean findNew;
    //increment strict
    private boolean strict;
    //position file
    private String positionFile;
    //idle max time
    private long idleMax;
    //idle interval;
    private long idleInterval;
    //table where
    private String where;

    private String defaultValue(DataType dataType) {
        String result;
        if (dataType.isString()) {
            result = "";
        } else if (dataType.isDateTime()) {
            result = "1970-01-01 00:00:00";
        } else if (dataType.isNumeric()) {
            result = "0";
        } else {
            throw new JDBCSourceException("not support type for increment: " + dataType.toString());
        }
        return result;
    }

    @Override
    protected void initTable(DBContext<?> dbContext) {
        super.initTable(dbContext);
        this.positionFile = dbContext.getPositionPath() + "/" + name + ".json";
        for (String increment : increments) {
            if (!tableFields.contains(increment)) {
                throw new JDBCSinkException(name + " table has no increment column exists: " + increment);
            }
        }
        //load start
        if (increments.length == 1) {
            DataType dataType = table.field(increments[0]).getDataType();
            if (defaultStart == null) {
                defaultStart = defaultValue(dataType);
            }
            Comparable defaultObj = (Comparable) dataType.convert(defaultStart);
            String position = readPosition();
            if (position != null && defaultObj.compareTo(dataType.convert(position)) < 0) {
                defaultStart = position;
            }
        } else {
            DataType incrementType = table.field(increments[1]).getDataType();
            Map<String, String> dbPosition = readDBPosition();
            Map<String, String> filePosition = new HashMap<>();
            String filePositionStr = readPosition();
            if (!Strings.isNullOrEmpty(filePositionStr)) {
                filePosition = JsonUtil.jsonToStringMap(filePositionStr);
            }
            //dbStart defaultStart configStart  fileStart
            for (Map.Entry<String, String> entry : dbPosition.entrySet()) {
                String identify = entry.getKey();
                List<Comparable> startList = new ArrayList<>();
                if (!Strings.isNullOrEmpty(defaultStart)) {
                    startList.add((Comparable) incrementType.convert(defaultStart));
                }
                String configStart = start.get(identify);
                if (!Strings.isNullOrEmpty(configStart)) {
                    startList.add((Comparable) incrementType.convert(configStart));
                }
                String fileStart = filePosition.get(identify);
                if (!Strings.isNullOrEmpty(fileStart)) {
                    startList.add((Comparable) incrementType.convert(fileStart));
                }
                Object value;
                if (startList.size() > 0) {
                    value = Collections.max(startList);
                } else {
                    value = defaultValue(incrementType);
                }
                start.put(identify, value.toString());
            }
        }
    }

    private String rsToJson(ResultSet rs, ResultSetMetaData metaData) throws Exception {
        int columnCount = metaData.getColumnCount();
        JsonObject jsonObject = new JsonObject();
        for (int i = 1; i <= columnCount; i++) {
            String cName = metaData.getColumnLabel(i);
            String value = rs.getString(i);
            if (!Strings.isNullOrEmpty(value)) {
                jsonObject.addProperty(cName, value);
            }
        }
        return JsonUtil.gson.toJson(jsonObject);
    }

    List<Event> readEvents(final long batchSize) {
        final List<Event> result = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        if (increments.length == 1) {
            if (tableIdle != null) {
                long needIdleTime = idleInterval * tableIdle.idleCount;
                if (needIdleTime > idleMax) {
                    needIdleTime = idleMax;
                }
                long passBy = currentTime - tableIdle.idleTime;
                if (passBy < needIdleTime) {
                    log.debug("table: {}, run: {}, idle: {}", getName(), passBy, needIdleTime);
                    return result;
                }
            }
            final Object defaultObj = table.field(increments[0]).getDataType().convert(defaultStart);
            Condition condition = field(increments[0]).gt(defaultObj);
            if (!Strings.isNullOrEmpty(where)) {
                condition = condition.and(where);
            }
            final Select select = dbContext.dslContext()
                    .select(selectField())
                    .from(table)
                    .where(condition)
                    .orderBy(field(increments[0]))
                    .limit((int) batchSize);
            log.debug(select.getSQL());

            final String columnAlias = getColumnAlias(increments[0]);
            final MutableObject mutableObject = new MutableObject();
            dbContext.dslContext().connection(new ConnectionRunnable() {
                @Override
                public void run(Connection connection) throws Exception {
                    PreparedStatement ps = connection.prepareStatement(select.getSQL());
                    ps.setObject(1, defaultObj);
                    if (!dbContext.isSQLServer()) {
                        ps.setObject(2, batchSize);
                    }
                    ResultSet rs = ps.executeQuery();
                    ResultSetMetaData metaData = rs.getMetaData();
                    while (rs.next()) {
                        Map<String, String> header = new HashMap<>();
                        header.put("table", getAlias());
                        String row = rsToJson(rs, metaData);
                        mutableObject.setValue(rs.getObject(columnAlias));
                        result.add(EventBuilder.withBody(row, Charset.forName("UTF-8"), header));
                    }
                }
            });
            if (mutableObject.getValue() != null) {
                defaultStart = mutableObject.getValue().toString();
                tableIdle = null;
            } else {
                if (tableIdle == null) {
                    tableIdle = new IdentifyIdle();
                }
                tableIdle.addIdleCount();
            }
        } else {
            DataType identifyType = table.field(increments[0]).getDataType();
            DataType incrementType = table.field(increments[1]).getDataType();
            for (Map.Entry<String, String> entry : start.entrySet()) {
                String identify = entry.getKey();
                IdentifyIdle identifyIdle = idleMap.get(identify);
                if (identifyIdle != null) {
                    long needIdleTime = idleInterval * identifyIdle.idleCount;
                    if (needIdleTime > idleMax) {
                        needIdleTime = idleMax;
                    }
                    long passBy = currentTime - identifyIdle.idleTime;
                    if (passBy < needIdleTime) {
                        log.debug("table: {}, identify: {}, run: {}, idle: {}", getName(), identify, passBy, needIdleTime);
                        continue;
                    }
                }
                final Object identifyValue = identifyType.convert(identify);
                final Object incrementValue = incrementType.convert(entry.getValue());
                Condition condition = field(increments[0]).eq(identifyValue)
                        .and(field(increments[1]).gt(incrementValue));
                if (!Strings.isNullOrEmpty(where)) {
                    condition = condition.and(where);
                }
                final Select select = dbContext.dslContext()
                        .select(selectField())
                        .from(table)
                        .where(condition)
                        .orderBy(field(increments[1]))
                        .limit((int) batchSize);
                log.debug(select.getSQL());
                final String columnAlias = getColumnAlias(increments[1]);
                final MutableObject mutableObject = new MutableObject();
                dbContext.dslContext().connection(new ConnectionRunnable() {
                    @Override
                    public void run(Connection connection) throws Exception {
                        PreparedStatement ps = connection.prepareStatement(select.getSQL());
                        ps.setObject(1, identifyValue);
                        ps.setObject(2, incrementValue);
                        if (!dbContext.isSQLServer()) {
                            ps.setObject(3, batchSize);
                        }
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        while (rs.next()) {
                            Map<String, String> header = new HashMap<>();
                            header.put("table", getAlias());
                            String row = rsToJson(rs, metaData);
                            mutableObject.setValue(rs.getObject(columnAlias));
                            result.add(EventBuilder.withBody(row, Charset.forName("UTF-8"), header));
                        }
                    }
                });
                if (mutableObject.getValue() != null) {
                    String increment = mutableObject.getValue().toString();
                    start.put(identify, increment);
                    idleMap.remove(identify);
                } else {
                    IdentifyIdle idle = idleMap.get(identify);
                    if (idle == null) {
                        idle = new IdentifyIdle();
                        idleMap.put(identify, idle);
                    }
                    idle.addIdleCount();
                }
            }
        }
        return result;
    }

    protected List<Field> selectField() {
        List<Field> result = new ArrayList<>();
        Set<String> accessFields = accessColumnSet;
        if (accessFields.isEmpty()) {
            accessFields = tableFields;
        }
        accessFields.add(increments[0]);
        if (increments.length == 2) {
            accessFields.add(increments[1]);
        }
        for (String fieldName : accessFields) {
            Field field = DSL.field(fieldName);
            String convert = columnConvertMap.get(fieldName);
            if (convert != null) {
                field = DSL.field(convert);
            }
            String alias = columnAliasMap.get(fieldName);
            if (alias != null) {
                field = field.as(alias);
            }
            result.add(field);
        }
        return result;
    }

    void findNewIdentify() {
        if (increments.length != 1 && findNew && includes.size() == 0) {
            Map<String, String> dbPosition = readDBPosition();
            for (Map.Entry<String, String> entry : dbPosition.entrySet()) {
                String identify = entry.getKey();
                if (start.containsKey(identify)) {
                    continue;
                }
                start.put(identify, entry.getValue());
            }
        }
    }

    private Map<String, String> readDBPosition() {
        final Map<String, String> result = new HashMap<>();
        if (increments.length != 1) {
            //select identify, min(increment) from table where identify not in excludes group by identify
            SelectJoinStep selectJoinStep = dbContext.dslContext()
                    .select(field(increments[0]), min(field(increments[1])))
                    .from(table);
            SelectConditionStep selectConditionStep;
            DataType identifyType = table.field(increments[0]).getDataType();
            final Set<Object> includeOrExcludeObjSet = new HashSet<>();
            if (includes.size() > 0) {
                for (String include : includes) {
                    includeOrExcludeObjSet.add(identifyType.convert(include));
                }
                selectConditionStep = selectJoinStep.where(field(increments[0]).in(includeOrExcludeObjSet));
            } else if (excludes.size() == 0) {
                selectConditionStep = selectJoinStep.where(trueCondition());
            } else {
                for (String exclude : excludes) {
                    includeOrExcludeObjSet.add(identifyType.convert(exclude));
                }
                selectConditionStep = selectJoinStep.where(field(increments[0]).notIn(includeOrExcludeObjSet));
            }
            final Select select = selectConditionStep.groupBy(field(increments[0]));
            log.debug(select.getSQL());
            dbContext.dslContext().connection(new ConnectionRunnable() {
                @Override
                public void run(Connection connection) throws Exception {
                    PreparedStatement ps = connection.prepareStatement(select.getSQL());
                    Object[] objArray = includeOrExcludeObjSet.toArray();
                    for (int i = 0; i < objArray.length; i++) {
                        ps.setObject(i + 1, objArray[i]);
                    }
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        String identify = rs.getString(1);
                        String increment = rs.getString(2);
                        result.put(identify, increment);
                    }
                }
            });
        }
        return result;
    }

    void writePosition() {
        File file = new File(positionFile);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            String json = null;
            if (increments.length == 1) {
                json = defaultStart;
            } else if (increments.length == 2 && start.size() > 0) {
                json = new Gson().toJson(start);
            }
            if (json != null) {
                writer.write(json);
            }
        } catch (Throwable t) {
            log.error("Failed writing positionFile", t);
        } finally {
            try {
                if (writer != null) writer.close();
            } catch (IOException e) {
                log.error("Error: " + e.getMessage(), e);
            }
        }
    }

    private String readPosition() {
        String result = null;
        File file = new File(positionFile);
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            result = IOUtils.toString(reader);
        } catch (FileNotFoundException e) {
            log.info("File not found: " + positionFile + ", not updating position");
        } catch (IOException e) {
            log.error("Failed loading positionFile: " + positionFile, e);
        } finally {
            try {
                if (reader != null) reader.close();
            } catch (IOException e) {
                log.error("Error: " + e.getMessage(), e);
            }
        }
        return result;
    }

    public String[] getIncrements() {
        return increments;
    }

    public void setIncrements(String[] increments) {
        this.increments = increments;
    }

    public Set<String> getIncludes() {
        return includes;
    }

    public void setIncludes(Set<String> includes) {
        this.includes = includes;
    }

    public Set<String> getExcludes() {
        return excludes;
    }

    public void setExcludes(Set<String> excludes) {
        this.excludes = excludes;
    }

    public String getDefaultStart() {
        return defaultStart;
    }

    public void setDefaultStart(String defaultStart) {
        this.defaultStart = defaultStart;
    }

    public Map<String, String> getStart() {
        return start;
    }

    public void setStart(Map<String, String> start) {
        this.start = start;
    }

    public boolean isFindNew() {
        return findNew;
    }

    public void setFindNew(boolean findNew) {
        this.findNew = findNew;
    }

    public boolean isStrict() {
        return strict;
    }

    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    public long getIdleMax() {
        return idleMax;
    }

    public void setIdleMax(long idleMax) {
        this.idleMax = idleMax;
    }

    public long getIdleInterval() {
        return idleInterval;
    }

    public void setIdleInterval(long idleInterval) {
        this.idleInterval = idleInterval;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public Map<String, String> getColumnConvertMap() {
        return columnConvertMap;
    }

    public void setColumnConvertMap(Map<String, String> columnConvertMap) {
        this.columnConvertMap = columnConvertMap;
    }

    private static class IdentifyIdle {
        long idleTime = System.currentTimeMillis();

        int idleCount;

        void addIdleCount() {
            idleCount++;
        }
    }
}
