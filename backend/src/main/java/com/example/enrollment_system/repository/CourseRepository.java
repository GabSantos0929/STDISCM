package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.Course;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CourseRepository extends JpaRepository<Course, String> {
}
