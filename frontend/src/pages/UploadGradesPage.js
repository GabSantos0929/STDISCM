import React, { useState, useEffect } from 'react';
import axios from 'axios';

function UploadGradesPage() {
  const [courses, setCourses] = useState([]);
  const [sections, setSections] = useState([]);
  const [students, setStudents] = useState([]);
  const [selectedCourse, setSelectedCourse] = useState('');
  const [selectedSection, setSelectedSection] = useState('');
  const [studentId, setStudentId] = useState('');
  const [grade, setGrade] = useState('');
  const [message, setMessage] = useState('');

  const validGrades = ["0.0", "1.0", "1.5", "2.0", "2.5", "3.0", "3.5", "4.0"];

  // Fetch courses when the component loads
  useEffect(() => {
    const fetchCourses = async () => {
      try {
        const response = await axios.get('http://localhost:3000/courses'); // Replace with actual API
        setCourses(response.data);
      } catch (error) {
        console.error('Error fetching courses:', error);
      }
    };
    fetchCourses();
  }, []);

  // Fetch sections for a selected course
  useEffect(() => {
    if (selectedCourse) {
      const fetchSections = async () => {
        try {
          const response = await axios.get(`http://localhost:3000/courses/${selectedCourse}/sections`); // Replace with actual API
          setSections(response.data);
        } catch (error) {
          console.error('Error fetching sections:', error);
        }
      };
      fetchSections();
    }
  }, [selectedCourse]);

  // Fetch students for a selected section
  useEffect(() => {
    if (selectedSection) {
      const fetchStudents = async () => {
        try {
          const response = await axios.get(`http://localhost:3000/sections/${selectedSection}/students`); // Replace with actual API
          setStudents(response.data);
        } catch (error) {
          console.error('Error fetching students:', error);
        }
      };
      fetchStudents();
    }
  }, [selectedSection]);

  // Handle grade submission
  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!validGrades.includes(grade)) {
      setMessage('Invalid grade selection.');
      return;
    }

    try {
      const response = await axios.post('http://localhost:3000/grades', {
        course: selectedCourse,
        section: selectedSection,
        studentId,
        grade,
      });
      setMessage('Grade uploaded successfully!');
    } catch (error) {
      setMessage('Error uploading grade: ' + error.message);
    }
  };

  return (
    <div>
      <h2>Upload Grades</h2>

      <form onSubmit={handleSubmit}>
        {/* Course Selection */}
        <label>Course: </label>
        <select
          value={selectedCourse}
          onChange={(e) => setSelectedCourse(e.target.value)}
          required
        >
          <option value="">Select Course</option>
          {courses.map((course) => (
            <option key={course.courseCode} value={course.courseCode}>
              {course.courseCode} - {course.courseName}
            </option>
          ))}
        </select>
        <br />

        {/* Section Selection */}
        <label>Section: </label>
        <select
          value={selectedSection}
          onChange={(e) => setSelectedSection(e.target.value)}
          required
        >
          <option value="">Select Section</option>
          {sections.map((section) => (
            <option key={section.sectionId} value={section.sectionId}>
              {section.sectionId} - {section.room}
            </option>
          ))}
        </select>
        <br />

        {/* Student Selection */}
        <label>Student ID: </label>
        <select
          value={studentId}
          onChange={(e) => setStudentId(e.target.value)}
          required
        >
          <option value="">Select Student</option>
          {students.map((student) => (
            <option key={student.studentId} value={student.studentId}>
              {student.studentId} - {student.name}
            </option>
          ))}
        </select>
        <br />

        {/* Grade Input */}
        <label>Grade: </label>
          <select
            value={grade}
            onChange={(e) => setGrade(e.target.value)}
            required
          >
            <option value="">Select Grade</option>
            {validGrades.map((g) => (
              <option key={g} value={g}>
                {g}
              </option>
            ))}
          </select>
        <br />

        {/* Submit Button */}
        <button type="submit">Submit Grade</button>
      </form>

      {/* Message after submission */}
      {message && <p>{message}</p>}
    </div>
  );
}

export default UploadGradesPage;
