import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { jwtDecode } from 'jwt-decode';
import { handleLogout } from "../auth/Logout";

function DashboardPage() {
  const navigate = useNavigate();
  const token = localStorage.getItem('authToken');
  
  if (!token) {
    navigate("/login");
  }
  const decodedToken = jwtDecode(token);
  const role = decodedToken.role;

  return (
    <div>
      <h2>Dashboard</h2>
      <Link to="/courses" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
        View Course Offerings
      </Link>
      <br />
      {role === "Student" && (
        <>
          <Link to="/enroll" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
            Enrollment: Add Classes
          </Link>
          <br />
          <Link to="/grades" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
            View Grades
          </Link>
        </>
      )}
      {role === "Professor" && (
        <Link to="/upload-grades" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
          Upload Grades
        </Link>
      )}
      <br /><br />
      <button onClick={() => handleLogout(navigate)} style={{ padding: "8px", cursor: "pointer" }}>Logout</button>
    </div>
  );
}

export default DashboardPage;
