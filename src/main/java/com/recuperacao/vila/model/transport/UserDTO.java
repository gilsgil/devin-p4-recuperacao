package com.recuperacao.vila.model.transport;

import com.recuperacao.vila.model.User;

import java.util.List;
import java.util.Objects;

public class UserDTO {

    private String email;
    private String password;
    private List<String> roles;
    private Long habitanteId;

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

    public Long getHabitanteId() {
        return habitanteId;
    }

    public void setHabitanteId(Long habitanteId) {
        this.habitanteId = habitanteId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof User)) return false;
        User user = (User) o;
        return getEmail().equals(user.getEmail());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEmail());
    }
}
