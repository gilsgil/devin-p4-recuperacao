package com.recuperacao.vila.model.dao;

import com.recuperacao.vila.config.database.JDBCConfig;
import com.recuperacao.vila.model.transport.HabitanteCreationDTO;
import com.recuperacao.vila.model.transport.HabitanteDTO;
import com.recuperacao.vila.model.transport.HabitanteDetailedDTO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


@Repository
public class HabitanteDAO {

    // Lista todos os habitantes
    public List<HabitanteDTO> listAll() throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        Statement stmt = connection.createStatement();
        stmt.execute("SELECT * FROM habitantes");
        ResultSet rs = stmt.getResultSet();

        List<HabitanteDTO> habitantes = new ArrayList<>();

        while (rs.next()) {
            HabitanteDTO habitante = extractedHabitanteDTO(rs);
            habitantes.add(habitante);
        }

        stmt.close();
        connection.close();

        return habitantes;
    }

    // Lista um habitante através do ID
    public HabitanteDetailedDTO listById(Long id) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM habitantes WHERE id = ?");
        preparedStatement.setLong(1, id);
        preparedStatement.execute();
        ResultSet rs = preparedStatement.getResultSet();

        HabitanteDetailedDTO habitante = null;

        while (rs.next()) {
            habitante = extractedHabitanteDetailedDTO(rs);
        }

        preparedStatement.close();
        connection.close();

        return habitante;
    }

    // Lista todos os habitantes com idade superior ao valor informado na URL
    public List<HabitanteDTO> listByAgeGreatherThan(Integer idade) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM habitantes WHERE date_part('year', age(dataNascimento)) > ?");
        preparedStatement.setInt(1, idade);
        preparedStatement.execute();
        ResultSet rs = preparedStatement.getResultSet();

        List<HabitanteDTO> habitantes = new ArrayList<>();

        while (rs.next()) {
            HabitanteDTO habitante = extractedHabitanteDTO(rs);
            habitantes.add(habitante);
        }

        return habitantes;
    }

    // Lista todos os habitantes por mês de nascimento, informado na URL
    public List<HabitanteDTO> listByMonth(Integer mes) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM habitantes WHERE date_part('month', (dataNascimento)) = ?");
        preparedStatement.setInt(1, mes);
        preparedStatement.execute();
        ResultSet rs = preparedStatement.getResultSet();

        List<HabitanteDTO> habitantes = new ArrayList<>();

        while (rs.next()) {
            HabitanteDTO habitante = extractedHabitanteDTO(rs);
            habitantes.add(habitante);
        }

        return habitantes;
    }


    // Cria um novo habitante
    public HabitanteDTO createHabitante(HabitanteCreationDTO createHabitante) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO habitantes (nome, sobreNome, dataNascimento, renda, cpf) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setString(1, createHabitante.getNome());
        preparedStatement.setString(2, createHabitante.getSobreNome());
        preparedStatement.setDate(3, Date.valueOf(createHabitante.getDataNascimento()));
        preparedStatement.setDouble(4, createHabitante.getRenda());
        preparedStatement.setString(5, createHabitante.getCpf());

        preparedStatement.execute();

        HabitanteDTO habitante = null;

        ResultSet resultSet = preparedStatement.getGeneratedKeys();

        while (resultSet.next()) {
            Long id = resultSet.getLong("id");
            String nome = resultSet.getString("nome");
            String sobreNome = resultSet.getString("sobreNome");
            Date dataNascimento = resultSet.getDate("dataNascimento");
            Double renda = resultSet.getDouble("renda");
            String cpf = resultSet.getString("cpf");
            habitante = new HabitanteDTO(id, nome, sobreNome, dataNascimento.toLocalDate(), renda, cpf);
            habitante.setId(id);
        }

        return habitante;
    }

    // Deleta um habitante através do ID especificado
    public ResponseEntity<Long> deleteHabitante(Long id) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM habitantes WHERE id = ?");
        preparedStatement.setLong(1, id);
        var result = preparedStatement.executeUpdate();
        connection.commit();

        var status = new ResponseEntity<>(id, HttpStatus.OK);

        if (result == 0) {
            status = new ResponseEntity<>(id, HttpStatus.NOT_FOUND);
        }

        return status;
    }

    // Lista um habitante através do nome especificado
    public List<HabitanteDTO> listByName(String name) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM habitantes WHERE LOWER(nome) LIKE ?");
        preparedStatement.setString(1, "%" + name.toLowerCase() + "%");
        preparedStatement.execute();
        ResultSet rs = preparedStatement.getResultSet();

        List<HabitanteDTO> habitantes = new ArrayList<>();

        while (rs.next()) {
            HabitanteDTO habitante = extractedHabitanteDTO(rs);
            habitantes.add(habitante);
        }

        return habitantes;
    }


    // ResultSets
    public HabitanteDetailedDTO extractedHabitanteDetailedDTO(ResultSet resultSet) throws SQLException {
        String nome = resultSet.getString("nome");
        String sobrenome = resultSet.getString("sobreNome");
        Date dataNascimento = resultSet.getDate("dataNascimento");
        Double renda = resultSet.getDouble("renda");
        String cpf = resultSet.getString("cpf");
        HabitanteDetailedDTO habitante = new HabitanteDetailedDTO(nome, sobrenome, dataNascimento.toLocalDate(), renda, cpf);

        return habitante;
    }

    public HabitanteDTO extractedHabitanteDTO(ResultSet resultSet) throws SQLException {
        Long id = resultSet.getLong("id");
        String nome = resultSet.getString("nome");
        String sobrenome = resultSet.getString("sobreNome");
        Date dataNascimento = resultSet.getDate("dataNascimento");
        Double renda = resultSet.getDouble("renda");
        String cpf = resultSet.getString("cpf");
        HabitanteDTO habitante = new HabitanteDTO(id, nome, sobrenome, dataNascimento.toLocalDate(), renda, cpf);

        return habitante;
    }

}
