package com.ilivoo.flume.sink.phoenix;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.phoenix.util.DateUtil;

public enum DefaultKeyGenerator implements KeyGenerator {

    UUID  {

        @Override
        public String generate() {
           return String.valueOf(java.util.UUID.randomUUID());
        }
         
    },
    TIMESTAMP {

        @Override
        public String generate() {
            java.sql.Timestamp ts = new Timestamp(System.currentTimeMillis());
            return DateUtil.DEFAULT_DATE_FORMATTER.format(ts);
        }
        
    },
    DATE {
        
        @Override
        public String generate() {
            Date dt =  new Date(System.currentTimeMillis());
            return DateUtil.DEFAULT_DATE_FORMATTER.format(dt);
        } 
    },
    RANDOM {

        @Override
        public String generate() {
            return String.valueOf(new Random().nextLong());
        }
        
    },
    NANOTIMESTAMP {

        @Override
        public String generate() {
            return String.valueOf(System.nanoTime());
        }
        
    };
}
