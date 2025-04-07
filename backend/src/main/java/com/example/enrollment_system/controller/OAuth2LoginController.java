package com.example.enrollment_system.controller;

import com.example.enrollment_system.security.JwtTokenUtil;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.web.bind.annotation.*;
import java.util.HashMap;
import java.util.Map;

@RestController
public class OAuth2LoginController {

    @GetMapping("/loginSuccess")
    public Map<String, Object> handleLoginSuccess(OAuth2AuthenticationToken authentication) {
        String email = authentication.getPrincipal().getAttribute("email");
        String role = determineRoleFromEmail(email);
        String jwtToken = JwtTokenUtil.generateToken(email, role);

        Map<String, Object> response = new HashMap<>();
        response.put("email", email);
        response.put("role", role);
        response.put("jwt", jwtToken);

        return response;
    }

    private String determineRoleFromEmail(String email) {
        if (email.endsWith("@dlsu.edu.ph")) {
            if (email.contains("_")) {
                return "Student";
            } else if (email.contains(".")) {
                return "Professor";
            }
        }
        return "Guest";
    }
}
