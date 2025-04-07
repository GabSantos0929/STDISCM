package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Enrollment;
import com.example.enrollment_system.repository.EnrollmentRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/enrollments")
public class EnrollmentController {
    private final EnrollmentRepository enrollmentRepository;

    public EnrollmentController(EnrollmentRepository enrollmentRepository) {
        this.enrollmentRepository = enrollmentRepository;
    }

    // Get all enrollments
    @GetMapping
    public List<Enrollment> getAllEnrollments() {
        return enrollmentRepository.findAll();
    }

    // Get enrollment by ID
    @GetMapping("/{id}")
    public ResponseEntity<Enrollment> getEnrollmentById(@PathVariable Long id) {
        Optional<Enrollment> enrollment = enrollmentRepository.findById(id);
        return enrollment.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}
