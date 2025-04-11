package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Cart;
import com.example.enrollment_system.model.Enrollment;
import com.example.enrollment_system.model.Section;
import com.example.enrollment_system.model.User;
import com.example.enrollment_system.repository.CartRepository;
import com.example.enrollment_system.repository.EnrollmentRepository;
import com.example.enrollment_system.repository.SectionRepository;
import com.example.enrollment_system.repository.UserRepository;

import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/enrollments")
@Profile("enrollment")
public class EnrollmentController {

    @Autowired
    private CartRepository cartRepository;

    @Autowired
    private SectionRepository sectionRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private EnrollmentRepository enrollmentRepository;

    @PostMapping
    @Transactional
    public ResponseEntity<String> enrollInCourses(@RequestBody List<Cart> cartItems) {
        for (Cart cartItem : cartItems) {
            Optional<Section> sectionOpt = sectionRepository.findByClassNumber(cartItem.getClassNumber());

            if (sectionOpt.isEmpty()) {
                return ResponseEntity.status(400).body("Section not found: " + cartItem.getClassNumber());
            }
            Section section = sectionOpt.get();

            Optional<User> userOpt = userRepository.findById(cartItem.getUserId());

            if (userOpt.isEmpty()) {
                return ResponseEntity.status(400).body("User not found: " + cartItem.getUserId());
            }
            User user = userOpt.get();
            
            Enrollment enrollment = new Enrollment();
            enrollment.setStudent(user);
            enrollment.setSection(section);
            enrollmentRepository.save(enrollment);
            section.setEnrolled(section.getEnrolled() + 1);
            sectionRepository.save(section);
            cartRepository.deleteByIdAndClassNumber(cartItem.getUserId(), cartItem.getClassNumber());
        }
        return ResponseEntity.ok("Successfully enrolled in selected courses.");
    }
}
