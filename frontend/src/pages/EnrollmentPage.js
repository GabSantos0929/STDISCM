import React, { useState } from "react";
import axios from "axios";

function EnrollmentPage() {
  const [classNbr, setClassNbr] = useState("");
  const [section, setSection] = useState(null);
  const [error, setError] = useState("");
  const [successMessage, setSuccessMessage] = useState("");
  const [cart, setCart] = useState([]);

  const handleSearch = async () => {
    setError("");
    setSuccessMessage("");

    if (!classNbr.trim()) {
      setError("Please enter a Class Nbr.");
      return;
    }

    try {
      const response = await axios.get(`http://localhost:3000/sections/${classNbr}`);

      if (response.data) {
        setSection(response.data);
      } else {
        setError("Class not found.");
      }
    } catch (err) {
      setError("Error fetching class details. Please try again.");
    }
  };

  const handleAddToCart = () => {
    if (!section) {
      setError("No section selected.");
      return;
    }

    if (section.status === "closed") {
      setError("Cannot add to cart. The class is closed.");
      return;
    }

    setCart((prevCart) => {
      if (!prevCart.some((item) => item.classNumber === section.classNumber)) {
        return [...prevCart, section];
      }
      return prevCart;
    });

    setSuccessMessage(`Added ${section.course.courseCode} - ${section.sectionId} to cart`);
    setSection(null);
    setClassNbr("");
  };

  const handleEnroll = async () => {
    if (cart.length === 0) {
      setError("Your cart is empty. Add sections to your cart first.");
      return;
    }

    try {
      const enrollments = cart.map((section) =>
        axios.post("http://localhost:3000/enroll", { classNbr: section.classNumber })
      );
      await Promise.all(enrollments);

      setSuccessMessage("Successfully enrolled in all courses in your cart!");
      setCart([]);
    } catch (err) {
      setError("Enrollment failed. Please try again.");
    }
  };

  return (
    <div>
      <h2>Course Enrollment</h2>

      {/* Class Nbr Input */}
      <input
        type="text"
        value={classNbr}
        onChange={(e) => setClassNbr(e.target.value)}
        placeholder="Enter Class Nbr"
        style={{ padding: "8px", marginRight: "10px" }}
      />
      <button onClick={handleSearch} style={{ padding: "8px 12px", cursor: "pointer" }}>
        Search
      </button>

      {/* Display Error Message */}
      {error && <p style={{ color: "red" }}>{error}</p>}

      {/* Show Section Details */}
      {section && (
        <div style={{ marginTop: "20px", border: "1px solid #ddd", padding: "15px", width: "300px" }}>
          <p><strong>Course Code:</strong> {section.course.courseCode}</p>
          <p><strong>Course Name:</strong> {section.course.courseName}</p>
          <p><strong>Section ID:</strong> {section.sectionId}</p>
          <p><strong>Status:</strong> {section.status}</p>
          <p><strong>Enrollment Cap:</strong> {section.enrollmentCap}</p>
          <p><strong>Enrolled:</strong> {section.enrolled}</p>

          {section.status === "open" && (
            <div>
              <button
                onClick={handleAddToCart}
                style={{ padding: "8px 12px", cursor: "pointer", marginTop: "10px" }}
              >
                Add to Cart
              </button>
            </div>
          )}
        </div>
      )}

      {/* Display Cart */}
      <div style={{ marginTop: "20px" }}>
        <h3>Your Cart</h3>
        {cart.length === 0 ? (
          <p>Your cart is empty</p>
        ) : (
          <ul>
            {cart.map((section, index) => (
              <li key={index}>
                {section.course.courseCode} - {section.sectionId}
              </li>
            ))}
          </ul>
        )}

        {/* Enroll Button */}
        {cart.length > 0 && (
          <button
            onClick={handleEnroll}
            style={{ padding: "8px 12px", cursor: "pointer", marginTop: "10px" }}
          >
            Enroll Now
          </button>
        )}
      </div>

      {/* Success Message */}
      {successMessage && <p style={{ color: "green", marginTop: "15px" }}>{successMessage}</p>}
    </div>
  );
}

export default EnrollmentPage;
