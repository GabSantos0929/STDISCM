package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.Cart;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CartRepository extends JpaRepository<Cart, Long> {
    void deleteByIdAndClassNumber(int userId, String classNumber);
}
