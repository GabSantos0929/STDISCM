package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Course;
import com.example.enrollment_system.model.Section;
import com.example.enrollment_system.model.SectionSchedule;
import com.example.enrollment_system.repository.CourseRepository;
import com.example.enrollment_system.repository.SectionRepository;
import com.example.enrollment_system.repository.SectionScheduleRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class CourseController {

    @Autowired
    private CourseRepository courseRepository;

    @Autowired
    private SectionRepository sectionRepository;

    @Autowired
    private SectionScheduleRepository sectionScheduleRepository;

    @GetMapping("/courses")
    public List<CourseWithSections> getAllCourses() {
        List<Course> courses = courseRepository.findAll();
        List<CourseWithSections> courseWithSectionsList = new ArrayList<>();

        for (Course course : courses) {
            List<Section> sections = sectionRepository.findByCourseCourseCode(course.getCourseCode());
            List<SectionWithSchedules> sectionWithSchedulesList = new ArrayList<>();

            for (Section section : sections) {
                List<SectionSchedule> schedules = sectionScheduleRepository.findBySectionSectionId(section.getSectionId());
                sectionWithSchedulesList.add(new SectionWithSchedules(section, schedules));
            }

            courseWithSectionsList.add(new CourseWithSections(course, sectionWithSchedulesList));
        }

        return courseWithSectionsList;
    }

    public static class CourseWithSections {
        private Course course;
        private List<SectionWithSchedules> sections;

        public CourseWithSections(Course course, List<SectionWithSchedules> sections) {
            this.course = course;
            this.sections = sections;
        }

        public Course getCourse() {
            return course;
        }

        public void setCourse(Course course) {
            this.course = course;
        }

        public List<SectionWithSchedules> getSections() {
            return sections;
        }

        public void setSections(List<SectionWithSchedules> sections) {
            this.sections = sections;
        }
    }

    public static class SectionWithSchedules {
        private Section section;
        private List<SectionSchedule> schedules;

        public SectionWithSchedules(Section section, List<SectionSchedule> schedules) {
            this.section = section;
            this.schedules = schedules;
        }

        public Section getSection() {
            return section;
        }

        public void setSection(Section section) {
            this.section = section;
        }

        public List<SectionSchedule> getSchedules() {
            return schedules;
        }

        public void setSchedules(List<SectionSchedule> schedules) {
            this.schedules = schedules;
        }
    }
}
