package com.recuperacao.vila.controllers.rest;

import com.recuperacao.vila.controllers.service.HabitanteService;
import com.recuperacao.vila.model.transport.HabitanteCreationDTO;
import com.recuperacao.vila.model.transport.HabitanteDTO;
import com.recuperacao.vila.model.transport.HabitanteDetailedDTO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

@RestController
@RequestMapping(value = "/habitantes")
public class HabitanteRest {

    private final HabitanteService habitanteService;

    public HabitanteRest(HabitanteService habitanteService) {
        this.habitanteService = habitanteService;
    }

    // Cria um habitante através de um método POST (Body)
    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public HabitanteDTO create(@RequestBody HabitanteCreationDTO habitante) throws SQLException, IllegalAccessException {
        return habitanteService.create(habitante);
    }

    // Lista todos os habitantes
    @GetMapping
    public List<HabitanteDTO> listAll() throws SQLException {
        return habitanteService.listHabitantes();
    }

    // Lista um habitante através do ID
    @GetMapping(value = "/{id}")
    public HabitanteDetailedDTO listById(@PathVariable(value = "id") Long id) throws SQLException {
        return habitanteService.getById(id);
    }

    // Deleta um habitante através do ID especificado
    @DeleteMapping(value = "/delete/{id}")
    public ResponseEntity<Long> delete(@PathVariable("id") Long id) throws SQLException {
        return habitanteService.delete(id);
    }

    // Lista um habitante por idade maior que um valor especificado
    @GetMapping(value = "/idade/maior/{idade}")
    public List<HabitanteDTO> listByAge(@PathVariable(value = "idade") Integer idade) throws SQLException {
        return habitanteService.listByAge(idade);
    }

    // Lista um habitante através de um nome especificado
    @GetMapping(value = "/nome/{nome}")
    public List<HabitanteDTO> listByName(@PathVariable(value = "nome") String nome) throws SQLException {
        return habitanteService.listByName(nome);
    }

    // Lista todos os habitantes por idade especificada na URL
    @GetMapping(value = "/mes/{mes}")
    public List<HabitanteDTO> listByMonth(@PathVariable(value = "mes") Integer mes) throws SQLException {
        return habitanteService.listByMonth(mes);
    }

}
