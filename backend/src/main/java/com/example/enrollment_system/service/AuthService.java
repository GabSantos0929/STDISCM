package com.example.enrollment_system.service;

import com.example.enrollment_system.model.User;
import com.example.enrollment_system.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Date;

@Service
@Profile("auth")
public class AuthService {

    @Autowired
    private UserRepository userRepository;

    public String authenticateUser(String email, String password) {
        User user = userRepository.findByEmail(email)
                .orElseThrow(() -> new RuntimeException("User not found"));

        if (!password.equals(user.getPassword())) {
            throw new RuntimeException("Invalid credentials");
        }
        return generateToken(user);
    }

    private final String SECRET_KEY = "super-secret-key";

    private String generateToken(User user) {
        long expirationTime = 1000 * 60 * 60 * (24 * 1); // 1 day

        //userid => query against table => role

        return Jwts.builder()
                .setSubject(user.getEmail())
                .claim("role", determineRole(user.getEmail()))
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + expirationTime))
                .signWith(SignatureAlgorithm.HS256, SECRET_KEY)
                .compact();
    }

    public String determineRole(String email) {
        if (email.endsWith("@dlsu.edu.ph")) {
            String localPart = email.substring(0, email.indexOf("@"));

            if (localPart.contains("_")) {
                return "Student";
            }
            else if (localPart.contains(".")) {
                return "Professor";
            }
        }
        return "Guest";
    }
}
