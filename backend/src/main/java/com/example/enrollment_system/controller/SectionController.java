package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Section;
import com.example.enrollment_system.repository.SectionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/sections")
public class SectionController {
    private final SectionRepository sectionRepository;

    public SectionController(SectionRepository sectionRepository) {
        this.sectionRepository = sectionRepository;
    }

    // Get all sections
    @GetMapping
    public List<Section> getAllSections() {
        return sectionRepository.findAll();
    }

    // Get section by ID
    @GetMapping("/{sectionId}")
    public ResponseEntity<Section> getSectionById(@PathVariable String sectionId) {
        Optional<Section> section = sectionRepository.findById(sectionId);
        return section.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}
