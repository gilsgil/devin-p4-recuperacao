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
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM habitantes WHERE id = ?");
        ps.setLong(1, id);
        ps.execute();
        ResultSet rs = ps.getResultSet();

        HabitanteDetailedDTO habitante = null;

        while (rs.next()) {
            habitante = extractedHabitanteDetailedDTO(rs);
        }

        ps.close();
        connection.close();

        return habitante;
    }

    // Lista todos os habitantes com idade superior ao valor informado na URL
    public List<HabitanteDTO> listByAgeGreatherThan(Integer idade) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM habitantes WHERE date_part('year', age(dataNascimento)) > ?");
        ps.setInt(1, idade);
        ps.execute();
        ResultSet rs = ps.getResultSet();

        List<HabitanteDTO> habitantes = new ArrayList<>();

        while (rs.next()) {
            HabitanteDTO habitante = extractedHabitanteDTO(rs);
            habitantes.add(habitante);
        }

        ps.close();
        connection.close();

        return habitantes;
    }

    // Lista todos os habitantes por mês de nascimento, informado na URL
    public List<HabitanteDTO> listByMonth(Integer mes) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM habitantes WHERE date_part('month', (dataNascimento)) = ?");
        ps.setInt(1, mes);
        ps.execute();
        ResultSet rs = ps.getResultSet();

        List<HabitanteDTO> habitantes = new ArrayList<>();

        while (rs.next()) {
            HabitanteDTO habitante = extractedHabitanteDTO(rs);
            habitantes.add(habitante);
        }

        ps.close();
        connection.close();

        return habitantes;
    }


    // Cria um novo habitante
    public HabitanteDTO createHabitante(HabitanteCreationDTO createHabitante) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement ps = connection.prepareStatement("INSERT INTO habitantes (nome, sobreNome, dataNascimento, renda, cpf) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, createHabitante.getNome());
        ps.setString(2, createHabitante.getSobreNome());
        ps.setDate(3, Date.valueOf(createHabitante.getDataNascimento()));
        ps.setDouble(4, createHabitante.getRenda());
        ps.setString(5, createHabitante.getCpf());

        ps.execute();

        HabitanteDTO habitante = null;

        ResultSet rs = ps.getGeneratedKeys();

        while (rs.next()) {
            Long id = rs.getLong("id");
            String nome = rs.getString("nome");
            String sobreNome = rs.getString("sobreNome");
            Date dataNascimento = rs.getDate("dataNascimento");
            Double renda = rs.getDouble("renda");
            String cpf = rs.getString("cpf");
            habitante = new HabitanteDTO(id, nome, sobreNome, dataNascimento.toLocalDate(), renda, cpf);
            habitante.setId(id);
        }

        ps.close();
        connection.close();

        return habitante;
    }

    // Deleta um habitante através do ID especificado
    public ResponseEntity<Long> deleteHabitante(Long id) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement("DELETE FROM habitantes WHERE id = ?");
        ps.setLong(1, id);
        var result = ps.executeUpdate();
        connection.commit();

        var status = new ResponseEntity<>(id, HttpStatus.OK);

        if (result == 0) {
            status = new ResponseEntity<>(id, HttpStatus.NOT_FOUND);
        }

        ps.close();
        connection.close();

        return status;
    }

    // Lista um habitante através do nome especificado
    public List<HabitanteDTO> listByName(String name) throws SQLException {
        Connection connection = new JDBCConfig().getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM habitantes WHERE LOWER(nome) LIKE ?");
        ps.setString(1, "%" + name.toLowerCase() + "%");
        ps.execute();

        ResultSet rs = ps.getResultSet();

        List<HabitanteDTO> habitantes = new ArrayList<>();

        while (rs.next()) {
            HabitanteDTO habitante = extractedHabitanteDTO(rs);
            habitantes.add(habitante);
        }

        ps.close();
        connection.close();

        return habitantes;
    }

    public HabitanteDetailedDTO extractedHabitanteDetailedDTO(ResultSet resultSet) throws SQLException {
        String nome = resultSet.getString("nome");
        String sobrenome = resultSet.getString("sobreNome");
        Date dataNascimento = resultSet.getDate("dataNascimento");
        Double renda = resultSet.getDouble("renda");
        String cpf = resultSet.getString("cpf");

        return new HabitanteDetailedDTO(nome, sobrenome, dataNascimento.toLocalDate(), renda, cpf);
    }

    public HabitanteDTO extractedHabitanteDTO(ResultSet resultSet) throws SQLException {
        Long id = resultSet.getLong("id");
        String nome = resultSet.getString("nome");
        String sobrenome = resultSet.getString("sobreNome");
        Date dataNascimento = resultSet.getDate("dataNascimento");
        Double renda = resultSet.getDouble("renda");
        String cpf = resultSet.getString("cpf");

        return new HabitanteDTO(id, nome, sobrenome, dataNascimento.toLocalDate(), renda, cpf);
    }
}
