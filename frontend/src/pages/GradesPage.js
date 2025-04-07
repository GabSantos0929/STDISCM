import React, { useState, useEffect } from "react";

function GradesPage() {
  const [grades, setGrades] = useState([]);
  
  useEffect(() => {
    const fetchGrades = async () => {
      const response = await fetch("/grades/12112345");
      const data = await response.json();
      setGrades(data);
    };

    fetchGrades();
  }, []);

  return (
    <div>
      <h2>My Grades</h2>
      <table border="1" style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th>Course Code</th>
            <th>Course Title</th>
            <th>Grade</th>
            <th>Units</th>
          </tr>
        </thead>
        <tbody>
          {grades.length > 0 ? (
            grades.map((grade, index) => (
              <tr key={index}>
                <td>{grade.section.course.courseCode}</td>
                <td>{grade.section.course.courseName}</td>
                <td>{grade.grade}</td>
                <td>{grade.section.course.units}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan="4">No grades found.</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

export default GradesPage;
