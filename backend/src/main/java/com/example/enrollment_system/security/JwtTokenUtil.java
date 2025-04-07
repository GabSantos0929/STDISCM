package com.example.enrollment_system.security;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Date;

public class JwtTokenUtil {
    private static final String SECRET_KEY = "super-secret-key";

    public static String generateToken(String email, String role) {
        long expirationTime = 1000 * 60 * 60 * (24 * 1); // 1 day

        return Jwts.builder()
                .setSubject(email)
                .claim("role", role)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + expirationTime))
                .signWith(SignatureAlgorithm.HS256, SECRET_KEY)
                .compact();
    }

    // private String extractToken(HttpServletRequest request) {
    //     String token = request.getHeader("Authorization");
    //     if (token != null && token.startsWith("Bearer ")) {
    //         return token.substring(7);
    //     }
    //     return null;
    // }

    // private boolean isTokenExpired(String token) {
    //     Date expiration = Jwts.parser()
    //             .setSigningKey(SECRET_KEY)
    //             .parseClaimsJws(token)
    //             .getBody()
    //             .getExpiration();
    //     return expiration.before(new Date());
    // }
}
