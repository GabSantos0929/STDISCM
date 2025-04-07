package com.example.enrollment_system.repository;

import com.example.enrollment_system.model.SectionSchedule;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SectionScheduleRepository extends JpaRepository<SectionSchedule, Long> {
    List<SectionSchedule> findBySectionSectionId(String sectionId);
}
