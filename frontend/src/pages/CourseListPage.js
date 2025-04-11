import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useNavigate } from "react-router-dom";
import { handleLogout } from "../auth/Logout";

function CourseListPage() {
  const [courses, setCourses] = useState([]);
  const [filteredCourses, setFilteredCourses] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    const fetchCourses = async () => {
      try {
        const response = await axios.get('http://192.168.68.119:8082/courses');
        setCourses(response.data);
        setFilteredCourses([]);
      } catch (error) {
        alert(`Error fetching courses: ${error.message}. Redirecting back to the dashboard.`);
        navigate("/dashboard");
      }
    };
    fetchCourses();
  }, []);

  const handleSearch = (e) => {
    if (searchQuery.trim() === "") {
      setFilteredCourses([]);
    } else {
      const filtered = courses.filter((course) =>
        course.course.courseCode.toLowerCase() === searchQuery.toLowerCase()
      );
      setFilteredCourses(filtered);
    }
  };
  

  return (
    <div>
      <h2>Available Courses</h2>
      <div style={{ marginBottom: "20px" }}>
        <input
          type="text"
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          placeholder="Search by Course Code"
          style={{ padding: "8px", width: "250px", marginRight: "10px" }}
        />
        <button onClick={handleSearch} style={{ padding: "8px 12px", cursor: "pointer" }}>
          Search
        </button>
      </div>

      <table border="1" style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th>Class Nbr</th>
            <th>Course</th>
            <th>Section</th>
            <th>Day/s</th>
            <th>Time</th>
            <th>Room</th>
            <th>Enrl Cap</th>
            <th>Enrolled</th>
            <th>Remarks</th>
          </tr>
        </thead>
        <tbody>
          {filteredCourses.length > 0 ? (
            filteredCourses.map((courseData) => (
              courseData.sections.map((sectionData) => (
                sectionData.schedules.map((schedule, index) => (
                  <tr key={index}>
                    {index === 0 && (
                      <>
                        <td rowSpan={sectionData.schedules.length}>{sectionData.section.classNumber}</td>
                        <td rowSpan={sectionData.schedules.length}>{courseData.course.courseCode}</td>
                        <td rowSpan={sectionData.schedules.length}>{sectionData.section.sectionId}</td>
                      </>
                    )}
                    <td>{schedule.day}</td>
                    <td>{`${schedule.startTime} - ${schedule.endTime}`}</td>
                    <td>{schedule.room || ""}</td>
                    {index === 0 && (
                      <>
                        <td rowSpan={sectionData.schedules.length}>{sectionData.section.enrollmentCap}</td>
                        <td rowSpan={sectionData.schedules.length}>{sectionData.section.enrolled}</td>
                        <td rowSpan={sectionData.schedules.length}>{sectionData.section.remarks}</td>
                      </>
                    )}
                  </tr>
                ))
              ))
            ))
          ) : (
            <tr>
              <td colSpan="9">No courses found.</td>
            </tr>
          )}
        </tbody>
      </table>
      <br /><br />
      <button onClick={() => handleLogout(navigate)} style={{ padding: "8px", cursor: "pointer" }}>Logout</button>
    </div>
  );
}

export default CourseListPage;
