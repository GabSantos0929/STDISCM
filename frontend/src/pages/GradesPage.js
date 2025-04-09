import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { jwtDecode } from 'jwt-decode';
import { handleLogout } from "../auth/Logout";

function GradesPage() {
  const [grades, setGrades] = useState([]);
  const navigate = useNavigate();
  
  useEffect(() => {
    const token = localStorage.getItem('authToken');
    
    if (!token) {
      navigate("/login");
    }
    const decodedToken = jwtDecode(token);
    const userEmail = decodedToken.sub;

    const fetchGrades = async () => {
      const userResponse = await fetch(`http://localhost:8080/users/email/${userEmail}`);
      const userData = await userResponse.json();
      
      if (userData && userData.userId) {
        const gradesResponse = await fetch(`http://localhost:8080/grades/${userData.userId}`);
        const gradesData = await gradesResponse.json();
        setGrades(gradesData);
      }
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
                <td>{grade.course.courseCode}</td>
                <td>{grade.course.courseName}</td>
                <td>{(grade.grade).toFixed(1)}</td>
                <td>{grade.course.units}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan="4">No grades found.</td>
            </tr>
          )}
        </tbody>
      </table>
      <br /><br />
      <button onClick={() => handleLogout(navigate)} style={{ padding: "8px", cursor: "pointer" }}>Logout</button>
    </div>
  );
}

export default GradesPage;
