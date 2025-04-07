package com.example.enrollment_system.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ReactController {
    @RequestMapping(value = {"/{path:^(?!auth|api|static|index\\.html).*$}", "/"})
    public String redirectToIndex() {
        return "forward:/index.html";
    }
}
