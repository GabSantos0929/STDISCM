import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Link, useNavigate } from "react-router-dom";
import { jwtDecode } from 'jwt-decode';
import { handleLogout } from "../auth/Logout";

function DashboardPage() {
  const navigate = useNavigate();
  const [role, setRole] = useState("");

  useEffect(() => {
    const fetchUserRole = async () => {
      const token = localStorage.getItem("authToken");

      if (!token) {
        navigate("/");
        return;
      }
      const decodedToken = jwtDecode(token);
      const userId = decodedToken.sub;

      try {
        const response = await axios.get(`http://192.168.68.101:8081/users/${userId}`, {
          headers: {
            Authorization: `Bearer ${token}`
          }
        });
        setRole(response.data.role);
      } catch (error) {
        alert(`Error fetching role:" ${error.message}. Redirecting back to the login page.`);
        navigate("/");
      }
    };
    fetchUserRole();
  }, [navigate]);

  return (
    <div>
      <h2>Dashboard</h2>
      <Link to="/courses" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
        View Course Offerings
      </Link>
      <br />
      {role === "student" && (
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
      {role === "faculty" && (
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
