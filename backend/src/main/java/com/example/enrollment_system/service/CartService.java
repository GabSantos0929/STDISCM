package com.example.enrollment_system.service;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Profile("enrollment")
public class CartService {
    private final Map<Integer, Set<String>> userCarts = new HashMap<>();

    public void addToCart(int userId, String classNumber) {
        userCarts.putIfAbsent(userId, new HashSet<>());
        userCarts.get(userId).add(classNumber);
    }

    public void removeFromCart(int userId, String classNumber) {
        Set<String> cart = userCarts.get(userId);
        if (cart != null) cart.remove(classNumber);
    }

    public Set<String> getCart(int userId) {
        return userCarts.getOrDefault(userId, Collections.emptySet());
    }

    public void clearCart(int userId) {
        userCarts.remove(userId);
    }
}
