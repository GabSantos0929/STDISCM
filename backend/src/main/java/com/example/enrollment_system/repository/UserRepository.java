package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface UserRepository extends JpaRepository<User, Integer> {
    Optional<User> findById(int userId);
    Optional<User> findByEmail(String email);
}
