package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Cart;
import com.example.enrollment_system.model.Section;
import com.example.enrollment_system.repository.CartRepository;
import com.example.enrollment_system.repository.SectionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/cart")
public class CartController {

    @Autowired
    private CartRepository cartRepository;

    @Autowired
    private SectionRepository sectionRepository;

    @PostMapping("/add")
    public ResponseEntity<String> addToCart(@RequestBody Cart cartItem) {
        Optional<Section> section = sectionRepository.findByClassNumber(cartItem.getClassNumber());
        if (section.isPresent()) {
            cartRepository.save(cartItem);
            return ResponseEntity.ok("Added to cart successfully.");
        } else {
            return ResponseEntity.status(400).body("The class number entered is invalid.");
        }
    }

    @PostMapping("/remove")
    public ResponseEntity<String> removeFromCart(@RequestBody Cart cartItem) {
        cartRepository.deleteByEmailAndClassNumber(cartItem.getEmail(), cartItem.getClassNumber());
        return ResponseEntity.ok("Removed from cart successfully.");
    }

    @GetMapping("/{email}")
    public ResponseEntity<List<Cart>> getCart(@PathVariable String email) {
        List<Cart> cartItems = cartRepository.findByEmail(email);
        return ResponseEntity.ok(cartItems);
    }
}
