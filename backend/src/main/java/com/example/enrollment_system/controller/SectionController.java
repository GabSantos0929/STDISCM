package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Section;
import com.example.enrollment_system.repository.SectionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;

import java.util.Optional;

@RestController
@RequestMapping("/sections")
@Profile("enrollment")
public class SectionController {

    @Autowired
    private SectionRepository sectionRepository;

    @GetMapping("/class/{classNumber}")
    public ResponseEntity<Section> getSectionByClassNumber(@PathVariable String classNumber) {
        Optional<Section> section = sectionRepository.findByClassNumber(classNumber);
        return section.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}
