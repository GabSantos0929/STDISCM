package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.Section;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface SectionRepository extends JpaRepository<Section, String> {
    List<Section> findByCourseCourseCode(String courseCode);
    Optional<Section> findByClassNumber(String classNumber);
}
