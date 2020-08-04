package com.ilivoo.flume.sink.phoenix.serializer;

import static com.ilivoo.flume.sink.phoenix.FlumeConstants.CONFIG_REGULAR_EXPRESSION;
import static com.ilivoo.flume.sink.phoenix.FlumeConstants.IGNORE_CASE_CONFIG;
import static com.ilivoo.flume.sink.phoenix.FlumeConstants.IGNORE_CASE_DEFAULT;
import static com.ilivoo.flume.sink.phoenix.FlumeConstants.REGEX_DEFAULT;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.phoenix.schema.types.PDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class RegexEventSerializer extends BaseEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(RegexEventSerializer.class);
  
    private Pattern inputPattern;
    
    /**
     * 
     */
    @Override
    public void doConfigure(Context context) {
        final String regex    = context.getString(CONFIG_REGULAR_EXPRESSION, REGEX_DEFAULT);
        final boolean regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,IGNORE_CASE_DEFAULT);
        inputPattern = Pattern.compile(regex, Pattern.DOTALL + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
     }

     
    /**
     * 
     */
    @Override
    public void doInitialize() throws SQLException {
        // NO-OP
    }
    
   
    @Override
    public void upsertEvents(List<Event> events) throws SQLException {
       Preconditions.checkNotNull(events);
       Preconditions.checkNotNull(connection);
       Preconditions.checkNotNull(this.upsertStatement);
       
       boolean wasAutoCommit = connection.getAutoCommit();
       connection.setAutoCommit(false);
       try (PreparedStatement colUpsert = connection.prepareStatement(upsertStatement)) {
           String value = null;
           Integer sqlType = null;
           for(Event event : events) {
               byte [] payloadBytes = event.getBody();
               if(payloadBytes == null || payloadBytes.length == 0) {
                   continue;
               }
               String payload = new String(payloadBytes);
               Matcher m = inputPattern.matcher(payload.trim());
               
               if (!m.matches()) {
                 logger.debug("payload {} doesn't match the pattern {} ", payload, inputPattern.toString());  
                 continue;
               }
               if (m.groupCount() != colNames.size()) {
                 logger.debug("payload {} size doesn't match the pattern {} ", m.groupCount(), colNames.size());
                 continue;
               }
               int index = 1 ;
               int offset = 0;
               for (int i = 0 ; i <  colNames.size() ; i++,offset++) {
                   if (columnMetadata[offset] == null ) {
                       continue;
                   }
                   
                   value = m.group(i + 1);
                   sqlType = columnMetadata[offset].getSqlType();
                   Object upsertValue = PDataType.fromTypeId(sqlType).toObject(value);
                   if (upsertValue != null) {
                       colUpsert.setObject(index++, upsertValue, sqlType);
                   } else {
                       colUpsert.setNull(index++, sqlType);
                   }
                }
               
               //add headers if necessary
               Map<String,String> headerValues = event.getHeaders();
               for(int i = 0 ; i < headers.size() ; i++ , offset++) {
                
                   String headerName  = headers.get(i);
                   String headerValue = headerValues.get(headerName);
                   sqlType = columnMetadata[offset].getSqlType();
                   Object upsertValue = PDataType.fromTypeId(sqlType).toObject(headerValue);
                   if (upsertValue != null) {
                       colUpsert.setObject(index++, upsertValue, sqlType);
                   } else {
                       colUpsert.setNull(index++, sqlType);
                   }
               }
  
               if(autoGenerateKey) {
                   sqlType = columnMetadata[offset].getSqlType();
                   String generatedRowValue = this.keyGenerator.generate();
                   Object rowkeyValue = PDataType.fromTypeId(sqlType).toObject(generatedRowValue);
                   colUpsert.setObject(index++, rowkeyValue ,sqlType);
               } 
               colUpsert.execute();
           }
           connection.commit();
       } catch(Exception ex){
           logger.error("An error {} occurred during persisting the event ",ex.getMessage());
           throw new SQLException(ex.getMessage());
       } finally {
           if(wasAutoCommit) {
               connection.setAutoCommit(true);
           }
       }
       
    }

}
