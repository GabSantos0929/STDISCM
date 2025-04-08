package com.example.enrollment_system.controller;

import com.example.enrollment_system.service.AuthService;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/auth")
@Profile("auth")
public class AuthController {

    @Autowired
    private AuthService authService;

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody UserLoginRequest loginRequest) {
        try {
            String token = authService.authenticateUser(loginRequest.getEmail(), loginRequest.getPassword());
            return ResponseEntity.ok(new LoginResponse(token));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid credentials");
        }
    }

    @Data
    public static class UserLoginRequest {
        private String email;
        private String password;
    }

    @Data
    public static class LoginResponse {
        private String token;

        public LoginResponse(String token) {
            this.token = token;
        }
    }
}
