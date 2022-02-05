package com.recuperacao.vila.model.transport;

public class ReportDTO {

    private Double orcamento;
    private Double gastoTotal;
    private Double diferenca;
    private Double maiorCusto;

    public ReportDTO() {
    }

    public ReportDTO(Double ganho, Double gastoTotal, Double diferenca, Double maiorCusto) {
        this.orcamento = ganho;
        this.gastoTotal = gastoTotal;
        this.diferenca = diferenca;
        this.maiorCusto = maiorCusto;
    }

    public Double getOrcamento() {
        return orcamento;
    }

    public void setOrcamento(Double orcamento) {
        this.orcamento = orcamento;
    }

    public Double getGastoTotal() {
        return gastoTotal;
    }

    public void setGastoTotal(Double gastoTotal) {
        this.gastoTotal = gastoTotal;
    }

    public Double getDiferenca() {
        return diferenca;
    }

    public void setDiferenca(Double diferenca) {
        this.diferenca = diferenca;
    }

    public Double getMaiorCusto() {
        return maiorCusto;
    }

    public void setMaiorCusto(Double maiorCusto) {
        this.maiorCusto = maiorCusto;
    }

    @Override
    public String toString() {
        return "ReportDTO{" +
                "saldo=" + orcamento +
                ", gasto=" + gastoTotal +
                ", diferenca=" + diferenca +
                ", habitanteCustoso=" + maiorCusto +
                '}';
    }
}
