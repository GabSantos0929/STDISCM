// package com.example.enrollment_system.security;

// import io.jsonwebtoken.ExpiredJwtException;
// import jakarta.servlet.Filter;
// import jakarta.servlet.FilterChain;
// import jakarta.servlet.FilterConfig;
// import jakarta.servlet.ServletException;
// import jakarta.servlet.ServletRequest;
// import jakarta.servlet.ServletResponse;
// import jakarta.servlet.http.HttpServletRequest;
// import jakarta.servlet.http.HttpServletResponse;

// import java.io.IOException;

// public class JWTAuthenticationFilter implements Filter {
    
//     private final String HEADER = "Authorization";
//     private final String PREFIX = "Bearer ";

//     @Override
//     public void init(FilterConfig filterConfig) throws ServletException {
//     }

//     @Override
//     public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
//             throws IOException, ServletException {
        
//         HttpServletRequest httpServletRequest = (HttpServletRequest) request;
//         HttpServletResponse httpServletResponse = (HttpServletResponse) response;

//         String token = httpServletRequest.getHeader(HEADER);
//         if (token == null || !token.startsWith(PREFIX)) {
//             httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
//             httpServletResponse.getWriter().write("Unauthorized: No token found");
//             return;
//         }

//         try {
//             String jwt = token.substring(PREFIX.length());
//             if (!Jwt.isTokenExpired(jwt)) {
//                 String username = Jwt.extractUsername(jwt);
//                 // You can also extract and set other claims such as roles if needed
//                 httpServletRequest.setAttribute("username", username);
//             } else {
//                 httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
//                 httpServletResponse.getWriter().write("Unauthorized: Token expired");
//                 return;
//             }
//         } catch (ExpiredJwtException e) {
//             httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
//             httpServletResponse.getWriter().write("Unauthorized: Invalid token");
//             return;
//         }

//         chain.doFilter(request, response);
//     }

//     @Override
//     public void destroy() {
//     }
// }
