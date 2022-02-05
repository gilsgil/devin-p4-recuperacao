package com.recuperacao.vila.controllers.rest;

import com.recuperacao.vila.controllers.service.ReportService;
import com.recuperacao.vila.model.transport.ReportDTO;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

@RestController
@PreAuthorize("hasAnyRole('ADMIN')")
@RequestMapping("/report")
public class ReportRest {

    private final ReportService reportService;

    public ReportRest(ReportService reportService) {
        this.reportService = reportService;
    }

    // Lista o relatório
    @GetMapping
    public ReportDTO getReport() throws SQLException, IllegalAccessException {
        return reportService.getReport();
    }

}
