package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Grade;
import com.example.enrollment_system.model.User;
import com.example.enrollment_system.model.Course;
import com.example.enrollment_system.repository.GradeRepository;
import com.example.enrollment_system.repository.UserRepository;
import com.example.enrollment_system.repository.CourseRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;
import jakarta.transaction.Transactional;
import java.util.Optional;
import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/grades/faculty")
@Profile("grades_faculty")
public class UploadGradesController {

    @Autowired
    private GradeRepository gradeRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private CourseRepository courseRepository;

    @PostMapping
    @Transactional
    public ResponseEntity<?> uploadGrade(@RequestBody Map<String, String> payload) {
        String studentIdStr = payload.get("studentId");
        String courseCode = payload.get("course");
        String gradeStr = payload.get("grade");

        Optional<User> studentOpt = userRepository.findById(Integer.parseInt(studentIdStr));
        if (studentOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("message", "Invalid student ID."));
        }

        Optional<Course> courseOpt = courseRepository.findById(courseCode);
        if (courseOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("message", "Invalid course code."));
        }

        try {
            BigDecimal gradeValue = new BigDecimal(gradeStr);
            Grade grade = new Grade();
            grade.setStudent(studentOpt.get());
            grade.setCourse(courseOpt.get());
            grade.setGrade(gradeValue);

            gradeRepository.save(grade);
            return ResponseEntity.ok(Map.of("message", "Grade uploaded successfully."));
        } catch (NumberFormatException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("message", "Invalid grade format."));
        }
    }
}
