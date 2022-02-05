package com.recuperacao.vila.model.transport;

import java.time.LocalDate;
import java.util.Objects;

public class HabitanteDTO {

    private Long id;
    private String nome;
    private String sobreNome;
    private LocalDate dataNascimento;
    private Double renda;
    private String cpf;

    public HabitanteDTO(){

    }

    public HabitanteDTO(Long id, String nome, String sobreNome, LocalDate dataNascimento, Double renda, String cpf) {
        this.id = id;
        this.nome = nome;
        this.sobreNome = sobreNome;
        this.dataNascimento = dataNascimento;
        this.renda = renda;
        this.cpf = cpf;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getSobreNome() {
        return sobreNome;
    }

    public void setSobreNome(String sobreNome) {
        this.sobreNome = sobreNome;
    }

    public LocalDate getDataNascimento() {
        return dataNascimento;
    }

    public void setDataNascimento(LocalDate dataNascimento) {
        this.dataNascimento = dataNascimento;
    }

    public Double getRenda() {
        return renda;
    }

    public void setRenda(Double renda) {
        this.renda = renda;
    }

    public String getCpf() {
        return cpf;
    }

    public void setCpf(String cpf) {
        this.cpf = cpf;
    }
}
