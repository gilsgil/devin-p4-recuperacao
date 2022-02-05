package com.recuperacao.vila.model.dao;

import com.recuperacao.vila.config.database.JDBCConfig;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.List;

@Repository
public class UserDAO {

    // Cria um novo usuário com base em um habitante
    public void create(String email, String password, Long habitanteId) throws SQLException, IllegalAccessException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement ps = connection.prepareStatement("INSERT INTO \"user\" (email, password, habitante_id) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, email);
        ps.setString(2, password);
        ps.setLong(3, habitanteId);
        ps.execute();
    }
}
