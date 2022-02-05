package com.recuperacao.vila.model.transport;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

public class HabitanteCreationDTO {

    private String nome;
    private String sobreNome;
    private LocalDate dataNascimento;
    private Double renda;
    private String cpf;
    private String email;
    private String password;
    private List<String> roles;

    public HabitanteCreationDTO(String nome, String sobreNome, LocalDate dataNascimento, Double renda, String cpf, String email, String password, List<String> roles) {
        super();
        this.nome = nome;
        this.sobreNome = sobreNome;
        this.dataNascimento = dataNascimento;
        this.renda = renda;
        this.cpf = cpf;
        this.email = email;
        this.password = password;
        this.roles = roles;
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

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HabitanteCreationDTO)) return false;
        HabitanteCreationDTO that = (HabitanteCreationDTO) o;
        return getCpf().equals(that.getCpf()) && getEmail().equals(that.getEmail());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCpf(), getEmail());
    }

    @Override
    public String toString() {
        return "HabitanteCreationDTO{" + "nome='" + nome + '\'' + ", sobreNome='" + sobreNome + '\'' + ", dataNascimento=" + dataNascimento + ", renda=" + renda + ", cpf='" + cpf + '\'' + ", email='" + email + '\'' + ", password='" + password + '\'' + ", roles=" + roles + '}';
    }
}
