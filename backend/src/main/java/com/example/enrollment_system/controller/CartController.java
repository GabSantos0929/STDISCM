package com.example.enrollment_system.controller;

import com.example.enrollment_system.model.Cart;
import com.example.enrollment_system.model.Section;
import com.example.enrollment_system.repository.SectionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/cart")
@Profile("enrollment")
public class CartController {

    private final Map<Integer, Set<String>> userCarts = new HashMap<>();

    @Autowired
    private SectionRepository sectionRepository;

    @PostMapping("/add")
    public ResponseEntity<String> addToCart(@RequestBody Cart cartItem) {
        Optional<Section> section = sectionRepository.findByClassNumber(cartItem.getClassNumber());
        if (section.isEmpty()) {
            return ResponseEntity.badRequest().body("Invalid class number.");
        }

        userCarts.putIfAbsent(cartItem.getUserId(), new HashSet<>());

        if (!userCarts.get(cartItem.getUserId()).add(cartItem.getClassNumber())) {
            return ResponseEntity.badRequest().body("Class already in cart.");
        }

        return ResponseEntity.ok("Added to cart successfully.");
    }

    @PostMapping("/remove")
    public ResponseEntity<String> removeFromCart(@RequestBody Cart cartItem) {
        Set<String> cart = userCarts.get(cartItem.getUserId());
        if (cart != null && cart.remove(cartItem.getClassNumber())) {
            return ResponseEntity.ok("Removed from cart successfully.");
        }
        return ResponseEntity.badRequest().body("Item not found in cart.");
    }

    @GetMapping("/{userId}")
    public ResponseEntity<Set<String>> getCart(@PathVariable int userId) {
        return ResponseEntity.ok(userCarts.getOrDefault(userId, Collections.emptySet()));
    }
}
