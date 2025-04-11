package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Grade;
import com.example.enrollment_system.model.User;
import com.example.enrollment_system.repository.GradeRepository;
import com.example.enrollment_system.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/grades/student")
@Profile("grades_student")
public class ViewGradesController {

    @Autowired
    private GradeRepository gradeRepository;

    @Autowired
    private UserRepository userRepository;

    @GetMapping("/{userId}")
    public ResponseEntity<List<Grade>> getGradesByUserId(@PathVariable int userId) {
        Optional<User> user = userRepository.findById(userId);
        return user.map(u -> ResponseEntity.ok(gradeRepository.findByStudent(u)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
