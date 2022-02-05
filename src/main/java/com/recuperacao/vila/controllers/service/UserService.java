package com.recuperacao.vila.controllers.service;

import com.recuperacao.vila.model.dao.UserDAO;
import com.recuperacao.vila.util.ValidationUtil;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

@Service
public class UserService {

    PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    private final UserDAO userDAO;

    public UserService(UserDAO userDAO) {
        this.userDAO = userDAO;
    }

    public void create(String email, String password, List<String> roles, Long habitanteId) throws SQLException, IllegalAccessException {
        if (!ValidationUtil.isValidUsername(email)) {
            throw new IllegalAccessException("Email inválido");
        }
        if (!ValidationUtil.isValidPassword(password)) {
            throw new IllegalAccessException("Password inválido");
        }

        passwordEncoder.encode(password);

        userDAO.create(email, password, habitanteId);
    }
}
