package com.example.enrollment_system.model;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "courses")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Course {
    @Id
    private String courseCode;

    @Column(nullable = false, length = 255)
    private String courseName;

    @Column(nullable = false)
    private int units;
}
