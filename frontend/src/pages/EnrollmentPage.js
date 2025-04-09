import React, { useState, useEffect } from "react";
import axios from "axios";
import { useNavigate } from "react-router-dom";
import { jwtDecode } from 'jwt-decode';
import { handleLogout } from "../auth/Logout";

function EnrollmentPage() {
  const [classNbr, setClassNbr] = useState("");
  const [email, setEmail] = useState(null);
  const [section, setSection] = useState(null);
  const [error, setError] = useState("");
  const [successMessage, setSuccessMessage] = useState("");
  const [cart, setCart] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    const token = localStorage.getItem('authToken');
        
    if (!token) {
      navigate("/login");
    }
    const decodedToken = jwtDecode(token);
    setEmail(decodedToken.sub); 
  }, []);

  const handleSearch = async () => {
    setError("");
    setSuccessMessage("");

    if (!classNbr.trim()) {
      setError("Please enter a Class Nbr.");
      return;
    }

    try {
      const response = await axios.get(`http://localhost:8081/sections/class/${classNbr}`);

      if (response.data) {
        setSection(response.data);
      } else {
        setError("Class not found.");
      }
    } catch (err) {
      setError("Error fetching class details. Please try again.");
    }
  };

  const handleAddToCart = async () => {
    if (!section) {
      setError("No section selected.");
      return;
    }
  
    if (section.status === "closed") {
      setError("Cannot add to cart. The class is closed.");
      return;
    }

    if (cart.some((item) => item.classNumber === section.classNumber)) {
      setError("This class is already in your cart.");
      return;
    }
  
    try {
      await axios.post("http://localhost:8081/cart/add", {
        email,
        classNumber: section.classNumber,
      });
  
      setCart((prevCart) => {
        if (!prevCart.some((item) => item.classNumber === section.classNumber)) {
          return [...prevCart, section];
        }
        return prevCart;
      });
  
      setSuccessMessage(`Added ${section.course.courseCode} - ${section.sectionId} to cart`);
      setSection(null);
      setClassNbr("");
    } catch (err) {
      setError("Error adding to cart. Please try again.");
    }
  };
  
  const handleRemoveFromCart = (classNumber) => {
    try {
      axios.post("http://localhost:8081/cart/remove", {
        email,
        classNumber,
      });
  
      setCart((prevCart) => prevCart.filter((item) => item.classNumber !== classNumber));
      setSuccessMessage(`Removed class ${classNumber} from cart`);
    } catch (err) {
      setError("Error removing from cart. Please try again.");
    }
  };  

  const handleEnroll = async () => {
    if (cart.length === 0) {
      setError("Your cart is empty. Add sections to your cart first.");
      return;
    }
    cart.forEach((item) => {
      item.email = email;
    });

    try {
      await axios.post("http://localhost:8081/enrollments", cart);
      setSuccessMessage("Successfully enrolled in all courses in your cart!");
      setCart([]);
    } catch (err) {
      console.error(err);
      setError("Enrollment failed. Please try again.");
    }
  };

  return (
    <div>
      <h2>Course Enrollment</h2>
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

      {error && <p style={{ color: "red" }}>{error}</p>}

      {section && (
        <div style={{ marginTop: "20px", border: "1px solid #ddd", padding: "15px", width: "300px" }}>
          <p><strong>Course Code:</strong> {section.course.courseCode}</p>
          <p><strong>Course Name:</strong> {section.course.courseName}</p>
          <p><strong>Section ID:</strong> {section.sectionId}</p>
          <p><strong>Status:</strong> {section.status}</p>
          <p><strong>Enrollment Cap:</strong> {section.enrollmentCap}</p>
          <p><strong>Enrolled:</strong> {section.enrolled}</p>
          <div>
            <button
              onClick={handleAddToCart}
              style={{ padding: "8px 12px", cursor: "pointer", marginTop: "10px" }}
            >
              Add to Cart
            </button>
          </div>
        </div>
      )}

      <div style={{ marginTop: "20px" }}>
        <h3>Your Cart</h3>
        {cart.length === 0 ? (
          <p>Your cart is empty</p>
        ) : (
          <ul>
            {cart.map((section, index) => (
              <li key={index}>
                {section.course.courseCode} - {section.sectionId}
                <button
                  onClick={() => handleRemoveFromCart(section.classNumber)}
                  style={{ marginLeft: "10px" }}
                >
                  Remove
                </button>
              </li>
            ))}
          </ul>
        )}

        {cart.length > 0 && (
          <button
            onClick={handleEnroll}
            style={{ padding: "8px 12px", cursor: "pointer", marginTop: "10px" }}
          >
            Enroll Now
          </button>
        )}
      </div>

      {successMessage && <p style={{ color: "green", marginTop: "15px" }}>{successMessage}</p>}
      <br /><br />
      <button onClick={() => handleLogout(navigate)} style={{ padding: "8px", cursor: "pointer" }}>Logout</button>
    </div>
  );
}

export default EnrollmentPage;
