import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useNavigate } from "react-router-dom";
import { handleLogout } from "../auth/Logout";

function UploadGradesPage() {
  const [course, setCourse] = useState('');
  const [section, setSection] = useState('');
  const [studentId, setStudentId] = useState('');
  const [grade, setGrade] = useState('');
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const validGrades = ["0.0", "1.0", "1.5", "2.0", "2.5", "3.0", "3.5", "4.0"];

  const handleSubmit = async (e) => {
    e.preventDefault();
    setMessage('');
    setError('');

    try {
      const response = await axios.post('http://localhost:8084/grades', {
        course,
        section,
        studentId,
        grade,
      });
      setMessage('Grade uploaded successfully!');
    } catch (err) {
      if (err.response?.data) {
        setError(err.response.data.message || 'An error occurred.');
      } else {
        setError('Unable to connect to the server.');
      }
    }
  };

  return (
    <div>
      <h2>Upload Grades</h2>
      <form onSubmit={handleSubmit}>
        <label>Course Code: </label>
        <input
          type="text"
          value={course}
          onChange={(e) => setCourse(e.target.value)}
          placeholder="e.g., STDISCM"
          required
        />
        <br />

        <label>Section ID: </label>
        <input
          type="text"
          value={section}
          onChange={(e) => setSection(e.target.value)}
          placeholder="e.g., S11"
          required
        />
        <br />

        <label>Student ID: </label>
        <input
          type="text"
          value={studentId}
          onChange={(e) => setStudentId(e.target.value)}
          placeholder="e.g., 12112345"
          required
        />
        <br />

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

        <button type="submit">Submit Grade</button>
      </form>

      {message && <p>{message}</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}
      <br /><br />
      <button onClick={() => handleLogout(navigate)} style={{ padding: "8px", cursor: "pointer" }}>Logout</button>
    </div>
  );
}

export default UploadGradesPage;
