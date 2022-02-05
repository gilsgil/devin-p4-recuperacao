package com.recuperacao.vila.auth;

import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ApplicationUserDAO {
    Optional<ApplicationUser> selectApplicationUserByEmail(String email);
}
