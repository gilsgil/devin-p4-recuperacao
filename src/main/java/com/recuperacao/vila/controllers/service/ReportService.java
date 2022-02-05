package com.recuperacao.vila.controllers.service;

import com.recuperacao.vila.model.transport.HabitanteDTO;
import com.recuperacao.vila.model.transport.ReportDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;


@Service
public class ReportService {

    @Value("${orcamento}")
    private Double orcamento;

    private final HabitanteService habitanteService;

    public ReportService(HabitanteService habitanteService) {
        this.habitanteService = habitanteService;
    }

    public ReportDTO getReport() throws SQLException, IllegalAccessException {
        Double gastoTotal = 0.0;

        List<HabitanteDTO> habitantes = habitanteService.listHabitantes();
        ReportDTO report = new ReportDTO();
        if (habitantes == null || habitantes.isEmpty()) {
            throw new IllegalAccessException("O relatório está vazio...");
        }

        report.setMaiorCusto(0.0);

        for (HabitanteDTO habitante : habitantes) {

            if (habitante.getRenda() > report.getMaiorCusto()) {
                report.setMaiorCusto(habitante.getRenda());

            }

            gastoTotal += habitante.getRenda();
            report.setGastoTotal(gastoTotal);
            report.setDiferenca(orcamento - gastoTotal);

            report.setOrcamento(orcamento);
        }

        System.out.println(report.getMaiorCusto());
        return report;
    }
}
