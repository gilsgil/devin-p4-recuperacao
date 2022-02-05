package com.recuperacao.vila.config.database;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Configuration
public class JDBCConfig {

    DataSource dataSource;
    String url = "jdbc:postgresql://localhost:5432/vila";
    String user = "postgres";
    String pass = "postgres";

    public JDBCConfig() {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setJdbcUrl(url);
        cpds.setUser(user);
        cpds.setPassword(pass);

        cpds.setMaxPoolSize(15);
        this.dataSource = cpds;
    }

    @Bean
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
