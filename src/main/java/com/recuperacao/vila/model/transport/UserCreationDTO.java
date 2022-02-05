package com.recuperacao.vila.model.transport;

import com.recuperacao.vila.model.User;
import org.springframework.data.annotation.Id;

import java.util.List;
import java.util.Objects;

public class UserCreationDTO {

    @Id
    private Long id;
    private String email;
    private String password;
    private List<String> roles;
    private Long habitanteId;

    public UserCreationDTO(Long id, String email, String password, List<String> roles, Long habitanteId) {
        this.id = id;
        this.email = email;
        this.password = password;
        this.roles = roles;
        this.habitanteId = habitanteId;
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

    public Long getHabitanteId() {
        return habitanteId;
    }

    public void setHabitanteId(Long habitanteId) {
        this.habitanteId = habitanteId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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
