import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import CourseListPage from './pages/CourseListPage';
import DashboardPage from "./pages/DashboardPage";
import EnrollmentPage from "./pages/EnrollmentPage";
import GradesPage from "./pages/GradesPage";
import UploadGradesPage from "./pages/UploadGradesPage";

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/courses" element={<CourseListPage />} />
        <Route path="/enroll" element={<EnrollmentPage />} />
        <Route path="/grades" element={<GradesPage />} />
        <Route path="/upload-grades" element={<UploadGradesPage />} />
      </Routes>
    </Router>
  );
}

export default App;
