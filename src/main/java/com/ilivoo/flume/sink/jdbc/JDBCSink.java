package com.ilivoo.flume.sink.jdbc;

import com.ilivoo.flume.jdbc.DBContext;
import com.ilivoo.flume.jdbc.JDBCHelper;
import com.ilivoo.flume.jdbc.JDBCTable;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.jooq.Configuration;
import org.jooq.TransactionalRunnable;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JDBCSink extends AbstractSink implements Configurable, BatchSizeSupported {

    public static final String DATA_FORMAT_HEAD = "head";
    public static final String DATA_FORMAT_BODY_JSON = "bodyJson";
    private static final Logger log = LoggerFactory.getLogger(JDBCSink.class);
    private static final String DEFAULT_DATA_FORMAT = DATA_FORMAT_HEAD;
    private static final String CONF_SQL = "sql";
    private static final String CONF_DATA_FORMAT = "dataFormat";
    private final CounterGroup counterGroup = new CounterGroup();
    private DBContext<JDBCTable> dbContext;
    private SinkCounter sinkCounter;
    private String dataFormat;
    private QueryGenerator queryGenerator;

    public JDBCSink() {
        super();
    }

    @Override
    public long getBatchSize() {
        return dbContext.getReadBatchSize();
    }

    @Override
    public void configure(Context context) {
        this.dataFormat = context.getString(CONF_DATA_FORMAT, DEFAULT_DATA_FORMAT);
        if (Arrays.asList(DATA_FORMAT_HEAD, DATA_FORMAT_BODY_JSON).contains(dataFormat)) {
            new JDBCSinkException("data Format not exist: " + dataFormat);
        }
        dbContext = JDBCHelper.create(context, JDBCTable.class);

        final String sql = context.getString(CONF_SQL);
        if (sql == null) {
            this.queryGenerator = new MappingQueryGenerator(dbContext, dataFormat, counterGroup);
        } else {
            this.queryGenerator = new TemplateQueryGenerator(
                    dbContext,
                    sql,
                    dataFormat,
                    counterGroup);
        }
        this.sinkCounter = new SinkCounter(this.getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
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
                final QueryGenerator tq = this.queryGenerator;
                dbContext.dslContext().transaction(new TransactionalRunnable() {
                    @Override
                    public void run(Configuration configuration) throws Throwable {
                        final boolean success = tq.executeQuery(DSL.using(configuration), eventList);
                        if (!success) {
                            throw new JDBCSinkException("Query failed");
                        }
                    }
                });
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
                throw new JDBCSinkException(t);
            }
        } finally {
            txn.close();
        }
        return status;
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
        log.info("JDBC Sink do stop. Metrics:{}", counterGroup);
    }
}
