package com.recuperacao.vila.auth;

import com.recuperacao.vila.config.database.JDBCConfig;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.*;

@Repository
public class ApplicationUserDAOImpl implements ApplicationUserDAO {

    private  final PasswordEncoder passwordEncoder;

    public ApplicationUserDAOImpl(PasswordEncoder passwordEncoder) {
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public Optional<ApplicationUser> selectApplicationUserByEmail(String email) {

        try {
            Connection connection = new JDBCConfig().getConnection();
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM \"user\" WHERE email = ?", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, email);
            ps.execute();
            ResultSet rs = ps.getResultSet();

            Optional<ApplicationUser> user = Optional.empty();

            while (rs.next()) {
                String username = rs.getString("email");
                String password = passwordEncoder.encode(rs.getString("password"));
                Array arrayRoles = rs.getArray("roles");
                String[] r = (String[]) arrayRoles.getArray();
                Set<String> roles = Set.of(r);
                user = Optional.of(new ApplicationUser(username, password, roles));
            }
            connection.close();
            return user;
        } catch (SQLException sqlException) {
            System.out.println("...");
        }
        return Optional.empty();
    }
}
