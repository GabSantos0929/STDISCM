import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

function HomePage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    try {
      const response = await fetch('http://localhost:8082/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          email: email,
          password: password,
        }),
      });

      if (response.ok) {
        const data = await response.json();
        const token = data.token; // Assuming the token is returned in the response

        // Store token and navigate to dashboard
        localStorage.setItem('authToken', token);
        navigate("/dashboard");
      } else {
        const errorMessage = await response.text();
        alert(errorMessage); // Show error message
      }
    } catch (error) {
      alert("An error occurred during login");
    }
  };

  return (
    <div>
      <h1>Welcome to MLS</h1>

      {/* Login Form */}
      <div>
        <h2>Login</h2>
        <form onSubmit={handleLogin}>
          <input
            type="email"
            placeholder="Email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            style={{ display: "block", marginBottom: "10px", padding: "8px" }}
            required
          />
          <input
            type="password"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            style={{ display: "block", marginBottom: "10px", padding: "8px" }}
            required
          />
          <button type="submit" style={{ padding: "10px 15px", cursor: "pointer" }}>
            Login
          </button>
        </form>
      </div>

      {/* Navigation Links */}
      <nav style={{ marginTop: "20px" }}>
        <a href="/courses" style={{ display: "block", margin: "10px 0", textDecoration: "none", color: "blue" }}>
          View Course Offerings
        </a>
      </nav>
    </div>
  );
}

export default HomePage;
