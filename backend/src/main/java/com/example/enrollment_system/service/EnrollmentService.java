// package com.example.enrollment_system.service;

// import com.example.enrollment_system.model.User;
// import com.example.enrollment_system.model.Course;
// import com.example.enrollment_system.model.Enrollment;
// import com.example.enrollment_system.model.Section;
// import com.example.enrollment_system.repository.CourseRepository;
// import com.example.enrollment_system.repository.EnrollmentRepository;
// import com.example.enrollment_system.repository.SectionRepository;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.stereotype.Service;

// import java.util.Optional;

// @Service
// public class EnrollmentService {

//     @Autowired
//     private SectionRepository sectionRepository;

//     @Autowired
//     private EnrollmentRepository enrollmentRepository;

    

//     // Enroll a student in a section
//     public Enrollment enrollStudentInSection(Long studentId, int classNumber) {
//         Optional<Section> sectionOpt = sectionRepository.findByClassNumber(classNumber);
//         if (!sectionOpt.isPresent()) {
//             throw new RuntimeException("Section not found");
//         }

//         Section section = sectionOpt.get();

//         // Check if section is open
//         if (section.getStatus() == Section.Status.closed) {
//             throw new RuntimeException("This section is closed.");
//         }

//         // Check enrollment cap
//         if (section.getEnrolled() >= section.getEnrollmentCap()) {
//             throw new RuntimeException("Section is full.");
//         }

//         // Create enrollment record
//         Enrollment enrollment = new Enrollment();
//         enrollment.setStudent(user); // You will need a User entity to associate
//         enrollment.setSection(section);
//         enrollmentRepository.save(enrollment);

//         // Update the number of enrolled students in the section
//         section.setEnrolled(section.getEnrolled() + 1);
//         sectionRepository.save(section);

//         return enrollment;
//     }

//     // Get section details by class number
//     public Section getSectionByClassNumber(int classNumber) {
//         Optional<Section> sectionOpt = sectionRepository.findByClassNumber(classNumber);
//         return sectionOpt.orElseThrow(() -> new RuntimeException("Section not found"));
//     }
// }
