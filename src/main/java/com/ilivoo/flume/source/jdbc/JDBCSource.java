package com.ilivoo.flume.source.jdbc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ilivoo.flume.jdbc.DBContext;
import com.ilivoo.flume.jdbc.JDBCHelper;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JDBCSource extends AbstractPollableSource
        implements Configurable, BatchSizeSupported {

    public static final String WRITE_POS_INTERVAL = "writePosInterval";
    public static final int DEFAULT_WRITE_POS_INTERVAL = 3000;
    public static final String FIND_NEW_INTERVAL = "findNewInterval";
    public static final int DEFAULT_FIND_NEW_INTERVAL = 5 * 60 * 1000;
    private static final Logger log = LoggerFactory.getLogger(JDBCSource.class);
    private int writePosInitDelay = 5000;
    private int writePosInterval;
    private ScheduledExecutorService positionService;

    private int findNewInitDelay = DEFAULT_FIND_NEW_INTERVAL;
    private int findNewInterval;
    private ScheduledExecutorService findNewService;

    private long DEFAULT_RETRY_INTERVAL = 1000;
    private long retryInterval = DEFAULT_RETRY_INTERVAL;
    private long maxRetryInterval = 10000;

    private CounterGroup counterGroup = new CounterGroup();

    private SourceCounter sourceCounter;

    private DBContext<JDBCSourceTable> dbContext;


    @Override
    protected void doConfigure(Context context) throws FlumeException {
        dbContext = JDBCHelper.create(context, JDBCSourceTable.class);
        writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
        if (writePosInterval < DEFAULT_WRITE_POS_INTERVAL) {
            writePosInterval = DEFAULT_WRITE_POS_INTERVAL;
        }
        findNewInterval = context.getInteger(FIND_NEW_INTERVAL, DEFAULT_FIND_NEW_INTERVAL);
        if (findNewInterval < DEFAULT_FIND_NEW_INTERVAL) {
            findNewInterval = DEFAULT_FIND_NEW_INTERVAL;
        }
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @Override
    public long getBatchSize() {
        return dbContext.getWriteBatchSize();
    }

    @Override
    protected Status doProcess() throws EventDeliveryException {
        Status status = Status.BACKOFF;
        try {
            for (JDBCSourceTable jdbcSourceTable : dbContext.getTables()) {
                List<Event> events = jdbcSourceTable.readEvents(getBatchSize());

                writeChannelProcess(events);

                if (events.size() >= getBatchSize()) {
                    status = Status.READY;
                } else {
                    log.debug("The events taken from " + jdbcSourceTable.getName() + " is less than " + getBatchSize());
                }
            }
        } catch (Throwable t) {
            log.error("Unable to tail files", t);
            sourceCounter.incrementEventReadFail();
            status = Status.BACKOFF;
        }
        return status;
    }

    private void writeChannelProcess(List<Event> events) throws InterruptedException {
        if (events == null || events.isEmpty()) {
            return;
        }
        sourceCounter.addToEventReceivedCount(events.size());
        sourceCounter.incrementAppendBatchReceivedCount();
        boolean loop = true;
        while (loop) {
            try {
                getChannelProcessor().processEventBatch(events);
            } catch (ChannelException ex) {
                log.warn("The channel is full or unexpected failure. " +
                        "The source will try again after " + retryInterval + " ms");
                sourceCounter.incrementChannelWriteFail();
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                retryInterval = retryInterval << 1;
                retryInterval = Math.min(retryInterval, maxRetryInterval);
                continue;
            }
            retryInterval = DEFAULT_RETRY_INTERVAL;
            loop = false;
        }
        log.debug("process {} event success", events.size());
        sourceCounter.incrementAppendBatchAcceptedCount();
        sourceCounter.addToEventAcceptedCount(events.size());
    }

    @Override
    protected void doStart() throws FlumeException {
        positionService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("positionService").build());
        positionService.scheduleWithFixedDelay(new PositionWriterRunnable(),
                writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

        findNewService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("findNewService").build());
        findNewService.scheduleWithFixedDelay(new FindNewRunnable(),
                findNewInitDelay, findNewInterval, TimeUnit.MILLISECONDS);

        sourceCounter.start();
        log.info("JDBC Source do start finished");
    }

    @Override
    protected void doStop() throws FlumeException {
        try {
            ExecutorService[] services = {positionService, findNewService};
            for (ExecutorService service : services) {
                service.shutdown();
                if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                    service.shutdownNow();
                }
            }
            // write the last position
            writePosition();
        } catch (InterruptedException e) {
            log.info("Interrupted while awaiting termination", e);
        }
        sourceCounter.stop();
        log.info("JDBC Source {} do stop. Metrics:{}", getName(), counterGroup);
    }

    private void writePosition() {
        for (JDBCSourceTable jdbcSourceTable : dbContext.getTables()) {
            jdbcSourceTable.writePosition();
        }
    }

    private class PositionWriterRunnable implements Runnable {
        @Override
        public void run() {
            writePosition();
        }
    }

    private class FindNewRunnable implements Runnable {
        @Override
        public void run() {
            for (JDBCSourceTable jdbcSourceTable : dbContext.getTables()) {
                jdbcSourceTable.findNewIdentify();
            }
        }
    }
}
