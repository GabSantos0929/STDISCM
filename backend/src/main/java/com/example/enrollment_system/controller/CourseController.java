package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Course;
import com.example.enrollment_system.model.Section;
import com.example.enrollment_system.model.SectionSchedule;
import com.example.enrollment_system.repository.CourseRepository;
import com.example.enrollment_system.repository.SectionRepository;
import com.example.enrollment_system.repository.SectionScheduleRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import lombok.Data;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@Profile("course")
public class CourseController {

    @Autowired
    private CourseRepository courseRepository;

    @Autowired
    private SectionRepository sectionRepository;

    @Autowired
    private SectionScheduleRepository sectionScheduleRepository;

    @GetMapping("/courses")
    public List<CourseWithSections> getAllCourses() {
        return courseRepository.findAll().stream()
                .map(course -> new CourseWithSections(course, getSectionsWithSchedules(course)))
                .collect(Collectors.toList());
    }

    private List<SectionWithSchedules> getSectionsWithSchedules(Course course) {
        return sectionRepository.findByCourseCourseCode(course.getCourseCode()).stream()
                .map(section -> new SectionWithSchedules(section, sectionScheduleRepository.findBySectionSectionId(section.getSectionId())))
                .collect(Collectors.toList());
    }

    @Data
    public static class CourseWithSections {
        private Course course;
        private List<SectionWithSchedules> sections;

        public CourseWithSections(Course course, List<SectionWithSchedules> sections) {
            this.course = course;
            this.sections = sections;
        }
    }

    @Data
    public static class SectionWithSchedules {
        private Section section;
        private List<SectionSchedule> schedules;

        public SectionWithSchedules(Section section, List<SectionSchedule> schedules) {
            this.section = section;
            this.schedules = schedules;
        }
    }
}
