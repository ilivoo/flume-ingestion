package com.ilivoo.flume.sink.cache;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ilivoo.flume.utils.JsonUtil;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CacheSink extends AbstractSink implements Configurable, BatchSizeSupported {

    private static final Logger log = LoggerFactory.getLogger(CacheSink.class);

    private static final String IN_HEADER = "header.";
    private static final String IN_VALUE = "value.";

    private static final String CACHE_NAME = "cacheName";
    private static final String CACHE_KEY = "cacheKey";

    private static final String BATCH_SIZE = "batchSize";

    public static final long DEFAULT_BATCH_SIZE = 100;

    private final CounterGroup counterGroup = new CounterGroup();
    private SinkCounter sinkCounter;

    private long batchSize;

    private String cacheName;
    private String cacheKey;

    public CacheSink() {
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

        this.cacheName = context.getString(CACHE_NAME);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(cacheName));

        this.cacheKey = context.getString(CACHE_KEY);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(cacheKey));

        this.sinkCounter = new SinkCounter(this.getName());
    }

    @Override
    public Status process() {
        log.debug("Executing CacheSink.process()...");
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
                    Map<String, String> valueMap = JsonUtil.jsonToStringMap(new String(event.getBody(), "UTF-8"));
                    String name = getValue(cacheName, headers, valueMap);
                    String key = getValue(cacheKey, headers, valueMap);
                    CacheHelper.put(name, key, valueMap);
                    log.debug("cache name {}, key {}, value {}", name, key, valueMap);
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
        this.sinkCounter.stop();
        log.info("OpenTSDB Sink do stop. Metrics:{}", counterGroup);
    }
}
