package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.Enrollment;
import com.example.enrollment_system.model.User;
import com.example.enrollment_system.model.Section;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EnrollmentRepository extends JpaRepository<Enrollment, Long> {
    List<Enrollment> findByStudent(User student);
    List<Enrollment> findBySection(Section section);
}
