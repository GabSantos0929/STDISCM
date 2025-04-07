package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.Grade;
import com.example.enrollment_system.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface GradeRepository extends JpaRepository<Grade, Long> {
    List<Grade> findByStudent(User student);
}
