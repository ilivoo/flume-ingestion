package com.ilivoo.flume.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.util.Properties;

public class HikariDataSourceProvider implements DataSourceProvider {

    private HikariConfig config;

    @Override
    public void configProps(Properties properties) {
        config = new HikariConfig(properties);
        if (config.getMaximumPoolSize() < 2) {
            config.setMaximumPoolSize(2);
        }
    }

    @Override
    public String getUrl() {
        return config.getJdbcUrl();
    }

    @Override
    public DataSource createDataSource() throws Exception {
        return new HikariDataSource(config);
    }
}
