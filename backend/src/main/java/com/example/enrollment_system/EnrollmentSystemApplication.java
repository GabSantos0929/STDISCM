package com.example.enrollment_system;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EnrollmentSystemApplication {


	public static void main(String[] args) {

		if (args.length > 0) {
			String serviceType = args[0];
			switch (serviceType) {
				case "1":
					System.setProperty("spring.profiles.active", "enrollment");
					System.setProperty("server.port", "8081");
					System.out.println("Enrollment Active");
					break;
				case "2":
					System.setProperty("spring.profiles.active", "auth");
					System.setProperty("server.port", "8082");
					System.out.println("Auth Active");
					break;
				case "3":
					System.setProperty("spring.profiles.active", "course");
					System.setProperty("server.port", "8083");
					System.out.println("Service Active");
					break;
				case "4":
					System.setProperty("spring.profiles.active", "grades_faculty");
					System.setProperty("server.port", "8084");
					System.out.println("Grades (Faculty) Active");
					break;
				case "5":
					System.setProperty("spring.profiles.active", "grades_student");
					System.setProperty("server.port", "8085");
					System.out.println("Grades (Student) Active");
					break;
				// Add other cases for different services
			}
		}

		SpringApplication.run(EnrollmentSystemApplication.class, args);

	}

}
