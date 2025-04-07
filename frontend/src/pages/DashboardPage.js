import React, { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { getToken, getRole, getEmail, storeAuthData } from '../auth/AuthProvider';

function DashboardPage() {
  const navigate = useNavigate();
  const [userDetails, setUserDetails] = useState({
    email: "",
    role: "",
  });
  const [isDataFetched, setIsDataFetched] = useState(false);

  useEffect(() => {
    const fetchUserData = async () => {
      try {
        const response = await fetch("http://localhost:8080/loginSuccess", { credentials: "include" });
        const data = await response.json();

        if (data.jwt) {
          storeAuthData(data);
          setIsDataFetched(true);
        } else {
          console.error("Login failed: No token received");
        }
        const test = getEmail();
        console.log(test);
      } catch (err) {
        console.error("Login failed:", err);
      }
    };
    fetchUserData();
  }, [navigate]);

  useEffect(() => {
    if (isDataFetched) {
      const token = getToken();
      if (!token) {
        console.error("No valid token found!");
      } else {
        setUserDetails({
          email: getEmail(),
          role: getRole(),
        });
      }
    }
  }, [isDataFetched]);

  return (
    <div>
      <h1>Welcome to MLS Dashboard</h1>
      <nav>
        <Link to="/courses">View Course Offerings</Link><br/>
        {userDetails.role === "Student" && (
          <>
            <Link to="/enroll">Enrollment: Add Classes</Link><br/>
            <Link to="/grades">View Grades</Link><br/>
          </>
        )}
        {userDetails.role === "Professor" && (
          <>
            <Link to="/upload-grades">Upload Grades</Link><br/>
          </>
        )}
      </nav>
    </div>
  );
}

export default DashboardPage;
