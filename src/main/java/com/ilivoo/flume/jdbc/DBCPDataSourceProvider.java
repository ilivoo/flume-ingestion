package com.ilivoo.flume.jdbc;

import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.util.Properties;

public class DBCPDataSourceProvider implements DataSourceProvider {

    private Properties properties;

    @Override
    public void configProps(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String getUrl() {
        return properties.getProperty("url");
    }

    @Override
    public DataSource createDataSource() throws Exception {
        return BasicDataSourceFactory.createDataSource(properties);
    }
}
