package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.User;
import com.example.enrollment_system.model.Grade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/grades")
public class GradeController {

    // @GetMapping("/{studentId}")
    // public List<Grade> getGrades(@PathVariable("studentId") int studentId) {
    //     User student = new User();
    //     student.setUserId(studentId);
    //     return gradeService.getGrades(student);
    // }
}
