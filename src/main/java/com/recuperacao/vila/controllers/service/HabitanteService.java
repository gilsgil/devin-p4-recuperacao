package com.recuperacao.vila.controllers.service;

import com.recuperacao.vila.model.transport.*;
import com.recuperacao.vila.model.dao.HabitanteDAO;
import com.recuperacao.vila.util.ValidationUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;


@Service
public class HabitanteService {

    @Autowired
    private HabitanteDAO habitanteDAO;

    @Autowired
    private UserService userService;

    // Lista um habitante por ID
    public HabitanteDetailedDTO getById(Long id) throws SQLException {
        HabitanteDetailedDTO habitante = this.habitanteDAO.listById(id);
        if (habitante == null) {
            throw new IllegalArgumentException("Habitante não encontrado...");
        }
        return habitante;
    }

    // Lista todos os habitantes
    public List<HabitanteDTO> listHabitantes() throws SQLException {
        List<HabitanteDTO> habitantes = this.habitanteDAO.listAll();
        if (habitantes.isEmpty()) {
            throw new IllegalArgumentException("Não foram encontrados habitantes...");
        }
        return habitantes;
    }

    // Lista todos os habitantes por mês de nascimento especificado na URL
    public List<HabitanteDTO> listByMonth(Integer mes) throws SQLException {
        List<HabitanteDTO> habitantes = this.habitanteDAO.listByMonth(mes);

        if (habitantes.isEmpty()) {
            throw new IllegalArgumentException("Nenhum habitante foi encontrado...");
        }
        return habitantes;
    }

    // Lista todos os habitantes com idade maior que o valor especificado na URL
    public List<HabitanteDTO> listByAge(Integer idade) throws SQLException {
        List<HabitanteDTO> habitante = this.habitanteDAO.listByAgeGreatherThan(idade);
        if (habitante == null) {
            throw new IllegalArgumentException("Não foram encontrados habitantes com idade maior que o valor especificado...");
        }
        return habitante;
    }

    // Cria um novo habitante
    public HabitanteDTO create(HabitanteCreationDTO habitante) throws SQLException, IllegalAccessException {
        if (habitante == null) {
            throw new IllegalAccessException("O habitante está nulo");
        }

        if (!ValidationUtil.isValidName(habitante.getNome())) {
            throw new IllegalAccessException("Nome inválido");
        }
        if (!ValidationUtil.isValidName(habitante.getSobreNome())) {
            throw new IllegalAccessException("Sobrenome inválido");
        }
        if (!ValidationUtil.isValidCPF(habitante.getCpf())) {
            throw new IllegalAccessException("CPF inválido");
        }

        HabitanteDTO novoHabitante = this.habitanteDAO.createHabitante(habitante);

        try {
            userService.create(habitante.getEmail(), habitante.getPassword(), habitante.getRoles(), novoHabitante.getId());
        } catch (Exception e) {
            habitanteDAO.deleteHabitante(novoHabitante.getId());
            throw e;
        }

        return novoHabitante;
    }

    // Deleta um habitante através do ID especificado
    public ResponseEntity<Long> delete(Long id) throws SQLException {
        return habitanteDAO.deleteHabitante(id);
    }

    public List<HabitanteDTO> listByName(String nome) throws SQLException {
        List<HabitanteDTO> habitantes = this.habitanteDAO.listByName(nome);

        if (habitantes.isEmpty()) {
            throw new IllegalArgumentException("Nenhum habitante encontrado...");
        }

        return habitantes;
    }
}
