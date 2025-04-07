import React from "react";
import { Link } from "react-router-dom";

function DashboardPage() {
    return (
        <div>
            <h2>Dashboard</h2>

            <Link to="/courses" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
                Enroll in Courses
            </Link>
            <br />
            <Link to="/grades" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
                View Grades
            </Link>
            <br />
            <Link to="/upload-grades" style={{ margin: "10px", textDecoration: "underline", color: "#007bff" }}>
                Upload Grades
            </Link>
        </div>
    );
}

export default DashboardPage;
