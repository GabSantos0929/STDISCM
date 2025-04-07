package com.example.enrollment_system.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalTime;

@Entity
@Table(name = "section_schedule")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SectionSchedule {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long scheduleId;

    @ManyToOne
    @JoinColumn(name = "section_id", nullable = false)
    private Section section;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ScheduleDay day;

    @Column(nullable = false)
    private LocalTime startTime;

    @Column(nullable = false)
    private LocalTime endTime;

    @Column(length = 10)
    private String room;

    public enum ScheduleDay {
        M, T, W, H, F, S
    }
}
