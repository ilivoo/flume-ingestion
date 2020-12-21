package com.ilivoo.flume.sink.opentsdb;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ilivoo.flume.utils.DateTimeUtil;
import com.ilivoo.flume.utils.JsonUtil;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.utils.Config;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TSDBSink extends AbstractSink implements Configurable, BatchSizeSupported {

    private static final Logger log = LoggerFactory.getLogger(TSDBSink.class);

    private static final String IN_HEADER = "header.";
    private static final String IN_VALUE = "value.";

    private static final String BATCH_SIZE = "batchSize";
    private static final String OPENTSDB_CONFIG = "opentsdb.config";
    private static final String OPENTSDB_CONFIG_PREFIX = "opentsdb.config.";
    private static final String METRIC_DATABASE = "metric.database";
    private static final String METRIC_TABLE = "metric.table";
    private static final String METRIC_TIME_COLUMN = "metric.time";
    private static final String METRIC_TAG_COLUMNS = "metric.tags";
    private static final String METRIC_VALUE_COLUMNS = "metric.values";

    public static final long DEFAULT_BATCH_SIZE = 100;

    private final CounterGroup counterGroup = new CounterGroup();
    private SinkCounter sinkCounter;

    private long batchSize;

    private String metricDatabase;

    private String metricTable;

    private String timeColumn;

    private String[] tagColumns;

    private String[] valueColumns;

    private TSDB tsdb;

    public TSDBSink() {
        super();
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

    @Override
    public void configure(Context context) {
        this.batchSize = context.getLong(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        Preconditions.checkArgument(batchSize > 0);

        this.metricDatabase = context.getString(METRIC_DATABASE);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(metricDatabase));

        this.metricTable = context.getString(METRIC_TABLE);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(metricTable));

        this.timeColumn = context.getString(METRIC_TIME_COLUMN);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(timeColumn));

        String strTag = context.getString(METRIC_TAG_COLUMNS);
        if (!Strings.isNullOrEmpty(strTag)) {
            this.tagColumns = strTag.split("\\s+");
        }
        Preconditions.checkArgument(tagColumns.length > 0);

        String strValue = context.getString(METRIC_VALUE_COLUMNS);
        if (!Strings.isNullOrEmpty(strValue)) {
            this.valueColumns = strValue.split("\\s+");
        }
        Preconditions.checkArgument(valueColumns.length > 0);

        this.tsdb = new TSDB(getConfig(context));

        this.sinkCounter = new SinkCounter(this.getName());
    }

    private Config getConfig(Context context) {
        Config config;
        String configPath = context.getString(OPENTSDB_CONFIG);
        try {
            if (!Strings.isNullOrEmpty(configPath)) {
                config = new Config(configPath);
            } else {
                config = new Config(true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<String, String> configMap = context.getSubProperties(OPENTSDB_CONFIG_PREFIX);
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            config.overrideConfig(entry.getKey(), entry.getValue());
        }
        return config;
    }

    @Override
    public Status process() {
        log.debug("Executing JDBCSink.process()...");
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();

        try {
            txn.begin();
            int count;
            final List<Event> eventList = new ArrayList<>();
            for (count = 0; count < getBatchSize(); ++count) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                eventList.add(event);
            }

            if (count <= 0) {
                sinkCounter.incrementBatchEmptyCount();
                counterGroup.incrementAndGet("channel.underflow");
                status = Status.BACKOFF;
            } else {
                if (count < getBatchSize()) {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                for (Event event : eventList) {
                    Map<String, String> headers = event.getHeaders();
                    String bodyJson = new String(event.getBody(), "UTF-8");
                    List<Map<String, String>> valueList = JsonUtil.jsonToStringMaps(bodyJson);

                    log.debug("headers {}", headers);
                    log.debug("body josn {}", bodyJson);

                    for (Map<String, String> eValue : valueList) {
                        String database = getValue(metricDatabase, headers, eValue).toLowerCase();
                        String table = getValue(metricTable, headers, eValue).toLowerCase();
                        long time = 0;
                        String datetime = eValue.get(timeColumn);
                        try {
                            time = DateTimeUtil.parseDateTimeString(datetime, null);
                        } catch (Exception e) {
                        }
                        if (time < 0) {
                            log.warn("time {} error, header {}, body {}", datetime, headers, bodyJson);
                            continue;
                        }
                        long timestamp = time / 1000 * 1000;
                        Map<String, String> tags = new HashMap<>();
                        for (String tag : tagColumns) {
                            String tagValue = eValue.get(tag);
                            if (!Strings.isNullOrEmpty(tagValue)) {
                                tags.put(tag, tagValue);
                            }
                        }
                        if (tags.size() <= 0) {
                            log.warn("no tag set, header {}, body {}", headers, bodyJson);
                            continue;
                        }
                        for (String valueColumn : valueColumns) {
                            String value = eValue.get(valueColumn);
                            if (Strings.isNullOrEmpty(value)) {
                                continue;
                            }
                            if (!NumberUtils.isNumber(value)) {
                                log.warn("metric column {} not a number, value is {}", valueColumn, value);
                                //log.warn("metric column {} not a number, value is {}, maps {}", valueColumn, value, eValue);
                                continue;
                            }
                            String metric = database + "." + table + "." + valueColumn.toLowerCase();
                            WritableDataPoints dp = getDataPoints(tsdb, metric, tags);
                            if (Tags.looksLikeInteger(value)) {
                                dp.addPoint(timestamp, Tags.parseLong(value));
                            } else {
                                dp.addPoint(timestamp, Float.parseFloat(value));
                            }
                        }
                    }
                }
                sinkCounter.addToEventDrainAttemptCount(count);
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(count);
            counterGroup.incrementAndGet("transaction.success");
            log.info("process {} event success", eventList.size());
        } catch (Throwable t) {
            log.error("Exception during process", t);
            txn.rollback();
            status = Status.BACKOFF;
            this.sinkCounter.incrementConnectionFailedCount();
            if (t instanceof Error) {
                throw new RuntimeException(t);
            }
        } finally {
            txn.close();
        }
        return status;
    }

    private String getValue(String key, Map<String, String> headers, Map<String, String> values) {
        String value = null;
        if (key.startsWith(IN_HEADER)) {
            String headerKey = key.substring(IN_HEADER.length());
            value = headers.get(headerKey);
        } else if (key.startsWith(IN_VALUE)) {
            String valueKey = key.substring(IN_VALUE.length());
            value = values.get(valueKey);
        }
        if (Strings.isNullOrEmpty(value)) {
            value = key;
        }
        return value;
    }

    @Override
    public synchronized void start() {
        super.start();
        this.sinkCounter.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        for (WritableDataPoints points : dataPoints.values()) {
            points.persist();
        }
        tsdb.shutdown();
        this.sinkCounter.stop();
        log.info("OpenTSDB Sink do stop. Metrics:{}", counterGroup);
    }

    private final HashMap<String, WritableDataPoints> dataPoints = new HashMap<>();

    private WritableDataPoints getDataPoints(final TSDB tsdb,
                                     final String metric,
                                     final Map<String, String> tags) {
        final String key = metric + tags;
        WritableDataPoints dp = dataPoints.get(key);
        if (dp != null) {
            return dp;
        }
        dp = tsdb.newDataPoints();
        dp.setSeries(metric, tags);
        dp.setBatchImport(true);
        dataPoints.put(key, dp);
        return dp;
    }
}
