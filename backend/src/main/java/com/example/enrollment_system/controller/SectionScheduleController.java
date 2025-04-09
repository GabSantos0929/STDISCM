package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.SectionSchedule;
import com.example.enrollment_system.repository.SectionScheduleRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/schedule")
@Profile("course")
public class SectionScheduleController {
    private final SectionScheduleRepository sectionScheduleRepository;

    public SectionScheduleController(SectionScheduleRepository sectionScheduleRepository) {
        this.sectionScheduleRepository = sectionScheduleRepository;
    }

    // Get all schedules
    @GetMapping
    public List<SectionSchedule> getAllSchedules() {
        return sectionScheduleRepository.findAll();
    }

    // Get schedule by ID
    @GetMapping("/{id}")
    public ResponseEntity<SectionSchedule> getScheduleById(@PathVariable Long id) {
        Optional<SectionSchedule> schedule = sectionScheduleRepository.findById(id);
        return schedule.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}
