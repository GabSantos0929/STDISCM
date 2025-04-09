package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.Cart;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CartRepository extends JpaRepository<Cart, Long> {
    List<Cart> findByEmail(String email);
    void deleteByEmailAndClassNumber(String email, String classNumber);
}
