package com.example.enrollment_system.model;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "sections")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Section {
    @Id
    private String sectionId;

    @Column(nullable = false, unique = true)
    private int classNumber;

    @ManyToOne
    @JoinColumn(name = "course_code", nullable = false)
    private Course course;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(nullable = false)
    private int enrollmentCap;

    @Column(nullable = false)
    private int enrolled;

    @Enumerated(EnumType.STRING)
    private Modality remarks;

    public enum Status {
        open, closed
    }

    public enum Modality {
        F2F, HYBRID, FULLONLINE
    }
}
