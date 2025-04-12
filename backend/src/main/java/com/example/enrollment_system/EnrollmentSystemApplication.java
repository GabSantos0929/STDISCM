package com.example.enrollment_system;

import org.slf4j.Logger; // Import SLF4J Logger
import org.slf4j.LoggerFactory; // Import SLF4J LoggerFactory
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext; // Import context
import org.springframework.core.env.Environment; // Import environment

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Arrays;


@SpringBootApplication
public class EnrollmentSystemApplication {

	// Define a logger for the class
	private static final Logger log = LoggerFactory.getLogger(EnrollmentSystemApplication.class);

	public static void main(String[] args) {
		String activeProfile = "default"; // Keep track of the active profile
		String serverPort = null; // Keep track of the explicitly set port

		// --- Dynamically set profile and port based on args ---
		if (args.length > 0) {
			String serviceType = args[0];
			switch (serviceType) {
				case "1":
					activeProfile = "auth";
					serverPort = "8081";
					break;
				case "2":
					activeProfile = "course";
					serverPort = "8082";
					break;
				case "3":
					activeProfile = "enrollment";
					serverPort = "8083";
					break;
				case "4":
					activeProfile = "grades_student";
					serverPort = "8084";
					break;
				case "5":
					activeProfile = "grades_faculty";
					serverPort = "8085";
					break;
				default:
				    log.warn("Unknown service type argument: {}. Running with default profile and port.", serviceType);
                    // Allow falling through to use default/property file values
                    activeProfile = null; // Indicate no specific profile set via args
                    serverPort = null;    // Indicate no specific port set via args
				    break;
			}

            // Set properties if a valid service type was matched
            if (activeProfile != null && serverPort != null) {
                 System.setProperty("spring.profiles.active", activeProfile);
                 System.setProperty("server.port", serverPort);
				 System.setProperty("server.address", "0.0.0.0");
                 log.info("Profile '{}' activated, port set to {} via command line argument.", activeProfile, serverPort);
            }

		} else {
		    log.info("No service type argument provided. Running with default profile and port defined in application properties.");
		}

		// --- Run the Spring application ---
		// The System Properties set above will be picked up by Spring's Environment
		ConfigurableApplicationContext context = SpringApplication.run(EnrollmentSystemApplication.class, args);

		// --- Get Environment AFTER context is initialized ---
		Environment environment = context.getBean(Environment.class);

		// --- Read configuration from Environment and print details ---
		String configuredAddress = environment.getProperty("server.address", "not specified (defaults depend on server)");
		// This will now correctly reflect the value from System.setProperty if it was set,
		// otherwise it falls back to application.properties or the default (like 8080)
		String configuredPortStr = environment.getProperty("server.port", "8080");
        String[] currentProfiles = environment.getActiveProfiles();
         if (currentProfiles.length == 0) {
            currentProfiles = environment.getDefaultProfiles();
        }


        log.info("--- Application Configuration Summary ---");
        log.info("Active profile(s): {}", Arrays.toString(currentProfiles));
		log.info("Configured Address (from server.address property): {}", configuredAddress);
		log.info("Configured Port (from server.port property / system property): {}", configuredPortStr);

		// --- Logic to interpret 0.0.0.0 and suggest URLs ---
        if ("0.0.0.0".equals(configuredAddress) || "::".equals(configuredAddress)) {
             log.info("Application configured to listen on ALL network interfaces based on server.address.");
             log.info("Potentially accessible URLs (verify with actual server logs & check firewalls):");
            try {
                int portNum = Integer.parseInt(configuredPortStr);

                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface iface = interfaces.nextElement();
                    // filter out loopback and disabled interfaces
                    if (iface.isLoopback() || !iface.isUp()) {
                        continue;
                    }
                    Enumeration<InetAddress> addresses = iface.getInetAddresses();
                    while(addresses.hasMoreElements()) {
                        InetAddress addr = addresses.nextElement();
                        // Optional: Filter for specific IP types if needed (e.g., IPv4)
                        // if (addr instanceof java.net.Inet4Address) {
                             log.info(" -> http://{}:{}/", addr.getHostAddress(), portNum);
                        // }
                    }
                }
                 log.info(" -> http://localhost:{}/", portNum); // Usually works with 0.0.0.0 bind
                 log.info(" -> http://127.0.0.1:{}/", portNum); // Explicit loopback

            } catch (NumberFormatException e) {
                 log.error("Could not parse configured port '{}' as an integer.", configuredPortStr);
            } catch (Exception e) {
                log.warn("Could not enumerate network interfaces to list potential URLs.", e);
            }
        } else if (!"not specified (defaults depend on server)".equals(configuredAddress)) {
            // Configured for a specific address
            log.info("Application configured to listen on specific address: {}", configuredAddress);
            log.info("Configured URL: http://{}:{}/", configuredAddress, configuredPortStr);
        } else {
             // server.address not explicitly set
             log.warn("server.address property not explicitly set. Default behaviour (e.g., Tomcat) usually binds to localhost/127.0.0.1.");
             log.info("Likely configured URL: http://localhost:{}/", configuredPortStr);
        }
        log.info("---------------------------------------");

		// The application continues running in the main thread for web applications
		// until it's shut down.
	}
}
