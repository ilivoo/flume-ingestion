package com.ilivoo.flume.source.jdbc;

import com.ilivoo.flume.utils.JsonUtil;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SourceRunner;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;

public class TestJDBCSourceTable {

    String connectionString = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false&user=root&password=root";
    //mysql8 com.mysql.cj.jdbc.Driver
    String driver = "com.mysql.jdbc.Driver";
    Connection conn;
    String positionDir = ".flume";

    @Before
    public void before() throws Exception {
        Class.forName(driver).newInstance();
        conn = DriverManager.getConnection(connectionString);
    }

    private void deletePositionDir() throws Exception {
        final Path path = Paths.get(positionDir);
        if (!Files.exists(path)) {
            return;
        }
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                FileVisitResult result = super.visitFile(file, attrs);
                Files.deleteIfExists(file);
                return result;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                FileVisitResult result = super.postVisitDirectory(dir, exc);
                Files.deleteIfExists(dir);
                return result;
            }
        });
    }

    @After
    public void after() throws Exception {
        conn.close();
        deletePositionDir();
    }

    @Test
    public void testDBContext() throws Exception {
        try {
            conn.prepareStatement("DROP TABLE source_one").execute();
            conn.prepareStatement("CREATE TABLE source_one(myId datetime, myInteger int, myString varchar(50));").execute();
        } catch (Exception ex) {
            conn.prepareStatement("CREATE TABLE source_one(myId datetime, myInteger int, myString varchar(50));").execute();
        }
        conn.prepareStatement("insert into source_one(myId, myInteger, myString) values('2019-10-14 00:00:00', 1, '1')").execute();
        conn.prepareStatement("insert into source_one(myId, myInteger, myString) values('2019-10-15 00:00:00', 2, '2')").execute();

        deletePositionDir();
        Context ctx = new Context();
        ctx.put("driver", driver);
        ctx.put("conn.jdbcUrl", connectionString);
        ctx.put("batchSize", "1");
        ctx.put("positionDir", positionDir);
        ctx.put("idleMax", "60000");
        ctx.put("idleInterval", "1000");
        ctx.put("tables", "source_one");
        ctx.put("tables.source_one", "source_one_replace");
        ctx.put("tables.source_one.idleMax", "1800000");
        ctx.put("tables.source_one.idleInterval", "30000");
        ctx.put("tables.source_one.columns", "myInteger myString");
        ctx.put("tables.source_one.columns.myInteger", "myInteger_replace");
        ctx.put("tables.source_one.increments", "myId");
        ctx.put("tables.source_one.increments.defaultStart", "2019-09-20 00:00:00");
        ctx.put("tables.source_one.increments.strict", "true");
        JDBCSource source = new JDBCSource();
        source.configure(ctx);

        Context cctx = new Context();
        cctx.put("capacity", "1000");
        cctx.put("transactionCapacity", "100");
        Channel channel = new MemoryChannel();
        Configurables.configure(channel, cctx);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(Collections.singletonList(channel));
        ChannelProcessor chp = new ChannelProcessor(rcs);
        source.setChannelProcessor(chp);

        SourceRunner runner = SourceRunner.forSource(source);
        runner.start();

        Thread.sleep(1000);
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        int eventNum = 0;
        while (eventNum < 2) {
            Event event = channel.take();
            if (event != null) {
                eventNum++;
                System.out.println(event.getHeaders());
                System.out.println(JsonUtil.jsonToObjectMap(new String(event.getBody(), "UTF-8")));
            }
        }

        transaction.commit();
        transaction.close();
        runner.stop();
    }

    @Test
    public void testDBContextWithTwoIncrement() throws Exception {
        try {
            conn.prepareStatement("DROP TABLE source_two").execute();
            conn.prepareStatement("CREATE TABLE source_two(myId varchar(20), myDate datetime, myInteger int, myString varchar(50));").execute();
        } catch (Exception ex) {
            conn.prepareStatement("CREATE TABLE source_two(myId varchar(20), myDate datetime, myInteger int, myString varchar(50));").execute();
        }
        conn.prepareStatement("insert into source_two(myId, myDate, myInteger, myString) values('1', '2019-10-14 00:00:00', 1, '1')").execute();
        conn.prepareStatement("insert into source_two(myId, myDate, myInteger, myString) values('2', '2019-10-14 00:00:00', 2, '2')").execute();
        conn.prepareStatement("insert into source_two(myId, myDate, myInteger, myString) values('3', '2019-10-14 00:00:00', 3, '3')").execute();
        conn.prepareStatement("insert into source_two(myId, myDate, myInteger, myString) values('4', '2019-10-14 00:00:00', 4, '4')").execute();
        conn.prepareStatement("insert into source_two(myId, myDate, myInteger, myString) values('5', '2019-10-14 00:00:00', 5, '5')").execute();

        deletePositionDir();
        Context ctx = new Context();
        ctx.put("driver", driver);
        ctx.put("conn.jdbcUrl", connectionString);
        ctx.put("batchSize", "1");
        ctx.put("positionDir", positionDir);
        ctx.put("idleMax", "60000");
        ctx.put("idleInterval", "1000");
        ctx.put("tables", "source_two");
        ctx.put("tables.source_two", "source_two_replace");
        ctx.put("tables.source_two.idleMax", "1800000");
        ctx.put("tables.source_two.idleInterval", "30000");
        ctx.put("tables.source_two.columns", "myInteger myString");
        ctx.put("tables.source_two.columns.myInteger", "myInteger_replace");
        ctx.put("tables.source_two.columns.myInteger.convert", "log2(myInteger)");
        ctx.put("tables.source_two.increments", "myId myDate");
        ctx.put("tables.source_two.increments.findNew", "true");
        ctx.put("tables.source_two.increments.excludes", "2 3");
        ctx.put("tables.source_two.increments.defaultStart", "2019-10-01 00:00:00");
        ctx.put("tables.source_two.increments.starts.1", "2019-09-28 00:00:00");
        ctx.put("tables.source_two.increments.starts.4", "2019-10-30 00:00:00");
        ctx.put("tables.source_two.increments.strict", "true");
        JDBCSource source = new JDBCSource();
        source.configure(ctx);

        Context cctx = new Context();
        cctx.put("capacity", "1000");
        cctx.put("transactionCapacity", "100");
        Channel channel = new MemoryChannel();
        Configurables.configure(channel, cctx);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(Collections.singletonList(channel));
        ChannelProcessor chp = new ChannelProcessor(rcs);
        source.setChannelProcessor(chp);

        SourceRunner runner = SourceRunner.forSource(source);
        runner.start();

        Thread.sleep(1000);
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        int eventNum = 0;
        while (eventNum < 2) {
            Event event = channel.take();
            if (event != null) {
                eventNum++;
                System.out.println(event.getHeaders());
                System.out.println(JsonUtil.jsonToObjectMap(new String(event.getBody(), "UTF-8")));
            }
        }

        transaction.commit();
        transaction.close();
        runner.stop();
    }
}
