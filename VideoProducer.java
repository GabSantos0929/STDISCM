import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
 
public class VideoProducer {
    private final int producerId;
    private final String sourceFolder;
    private final String serverAddress;
    private final int serverPort;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Set<String> processingFiles = new HashSet<>();
 
    // Add a shared set for duplicate file hashes across all producers
    private static final Set<String> knownDuplicateHashes = ConcurrentHashMap.newKeySet();
 
    private static final int SOCKET_CONNECT_TIMEOUT_MS = 30000; // 30 seconds connection timeout
    private static final int SOCKET_READ_TIMEOUT_MS = 60000; // 60 seconds read timeout
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 5000; // 5 seconds
    private static final int BUFFER_SIZE = 65536; // Increased buffer to 64KB
 
    public static void main(String[] args) {
        // Parse command line arguments
        int producerCount = 1;
        if (args.length > 0) {
            try {
                producerCount = Integer.parseInt(args[0]);
                System.out.println("Producer Thread = " + producerCount);
            } catch (NumberFormatException e) {
                System.err.println("Invalid Producer Thread arg, default P=1 is used.");
            }
        }
 
        else {
            System.out.println("Default Producer Thread = 1");
        }
 
        String serverAddress = "localhost";
        if (args.length > 1) {
            serverAddress = args[1];
            System.out.println("Server Address = " + serverAddress);
        }
 
        else {
            System.out.println("Default Server Address = localhost");
        }
 
        int serverPort = 8080;
        if (args.length > 2) {
            try {
                serverPort = Integer.parseInt(args[2]);
                System.out.println("Server Port = " + serverPort);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port, using default: 8080");
            }
        }
 
        else {
            System.out.println("Default Server Port = 8080");
        }
 
        // Create and start producer threads
        ExecutorService executor = Executors.newFixedThreadPool(producerCount);
        final VideoProducer[] producers = new VideoProducer[producerCount];
 
        for (int i = 0; i < producerCount; i++) {
            final int producerId = i;
            final String sourceFolder = "Video-" + producerId;
 
            // Ensure source folder exists
            new File(sourceFolder).mkdirs();
 
            // Create processed folder
            new File(sourceFolder, "processed").mkdirs();
 
            VideoProducer producer = new VideoProducer(producerId, sourceFolder, serverAddress, serverPort);
            producers[i] = producer;
 
            executor.submit(() -> {
                try {
                    producer.start();
                } catch (Exception e) {
                    System.err.println("Producer " + producerId + " encountered an error: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
 
        // Add shutdown hook to gracefully terminate producers
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down producers...");
            for (VideoProducer producer : producers) {
                producer.stop();
            }
 
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
            System.out.println("Producers shutdown complete");
        }));
    }
 
    public VideoProducer(int producerId, String sourceFolder, String serverAddress, int serverPort) {
        this.producerId = producerId;
        this.sourceFolder = sourceFolder;
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }
 
    public void stop() {
        running.set(false);
    }
 
    // Create a class to hold the result of sending a video
    private record SendResult(boolean success, boolean duplicate) {
    }
 
    // Add method to compute file hash
    private String computeFileHash(File file) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    md.update(buffer, 0, bytesRead);
                }
            }
            byte[] digest = md.digest();
 
            // Convert to hex string
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            System.err.println("Error computing file hash: " + e.getMessage());
            return ""; // Empty string on error
        }
    }
 
    public void start() throws InterruptedException {
        System.out.println("Producer " + producerId + " starting, watching folder: " + sourceFolder);
 
        File dir = new File(sourceFolder);
        File processedDir = new File(sourceFolder, "processed");
        processedDir.mkdirs();
 
        // Track duplicates to avoid retrying them
        Set<String> duplicateFiles = new HashSet<>();
 
        // Continuously watch for new files
        while (running.get()) {
            File[] videoFiles = dir.listFiles((d, name) -> {
                String lowerName = name.toLowerCase();
                return lowerName.endsWith(".mp4") || lowerName.endsWith(".avi") ||
                        lowerName.endsWith(".mkv") || lowerName.endsWith(".mov") ||
                        lowerName.endsWith(".wmv") || lowerName.endsWith(".flv") ||
                        lowerName.endsWith(".webm") || lowerName.endsWith(".m4v");
            });
 
            if (videoFiles != null) {
                for (File videoFile : videoFiles) {
                    if (!running.get()) break;
 
                    // Skip files that are known duplicates
                    if (duplicateFiles.contains(videoFile.getName())) {
                        continue;
                    }
 
                    // Skip files that are already being processed
                    synchronized (processingFiles) {
                        if (processingFiles.contains(videoFile.getName())) {
                            continue;
                        }
                        processingFiles.add(videoFile.getName());
                    }
 
                    try {
                        // Check if file is completely written
                        if (isFileReady(videoFile)) {
                            // Compute hash for duplicate detection
                            String fileHash = computeFileHash(videoFile);
                            if (!fileHash.isEmpty() && knownDuplicateHashes.contains(fileHash)) {
                                System.out.println("File " + videoFile.getName() +
                                        " matches a known duplicate hash - skipping");
 
                                // Mark as duplicate locally too
                                duplicateFiles.add(videoFile.getName());
 
                                // Move to processed folder
                                try {
                                    Path source = videoFile.toPath();
                                    Path target = new File(processedDir, videoFile.getName()).toPath();
                                    Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                                    System.out.println("Moved duplicate file " + videoFile.getName() +
                                            " to processed folder");
                                } catch (IOException e) {
                                    System.err.println("Failed to move processed file: " +
                                            videoFile.getName() + ": " + e.getMessage());
                                }
                                continue;
                            }
 
                            boolean success = false;
                            boolean isDuplicate = false;
                            int attempts = 0;
 
                            while (!success && attempts < MAX_RETRIES && running.get() && !isDuplicate) {
                                SendResult result = sendVideo(videoFile);
                                success = result.success();
                                isDuplicate = result.duplicate();
 
                                if (isDuplicate) {
                                    // Remember this file as a duplicate to avoid future retries
                                    duplicateFiles.add(videoFile.getName());
 
                                    // Add to global set of known duplicate hashes
                                    if (!fileHash.isEmpty()) {
                                        knownDuplicateHashes.add(fileHash);
                                        System.out.println("File hash for " + videoFile.getName() +
                                                " marked as duplicate globally");
                                    }
 
                                    System.out.println("File " + videoFile.getName() +
                                            " marked as duplicate - will not retry");
                                    break;
                                }
 
                                if (!success && !isDuplicate) {
                                    attempts++;
                                    if (attempts < MAX_RETRIES) {
                                        System.out.println("Retrying " + videoFile.getName() + " (attempt " +
                                                (attempts + 1) + " of " + MAX_RETRIES + ")");
                                        Thread.sleep(RETRY_DELAY_MS);
                                    }
                                }
                            }
 
                            if (success || isDuplicate) {
                                // After successful upload or if duplicate, move file to processed folder
                                try {
                                    Path source = videoFile.toPath();
                                    Path target = new File(processedDir, videoFile.getName()).toPath();
                                    Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                                } catch (IOException e) {
                                    System.err.println("Failed to move processed file: " +
                                            videoFile.getName() + ": " + e.getMessage());
                                }
                            } else if (attempts >= MAX_RETRIES) {
                                System.err.println("Failed to send " + videoFile.getName() +
                                        " after " + MAX_RETRIES + " attempts");
                            }
                        }
                    } finally {
                        // Remove from processing list regardless of outcome
                        synchronized (processingFiles) {
                            processingFiles.remove(videoFile.getName());
                        }
                    }
                }
            }
 
            // Sleep before checking again
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
 
        System.out.println("Producer " + producerId + " shutting down");
    }
 
    private boolean isFileReady(File file) {
        // Check if file exists and is not empty
        if (!file.exists() || file.length() == 0) {
            return false;
        }
 
        // Try to acquire an exclusive lock on the file to ensure it's not being written
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
             FileChannel channel = raf.getChannel()) {
 
            FileLock lock = null;
            try {
                // Try to acquire a non-blocking lock
                lock = channel.tryLock();
                return lock != null;
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        } catch (Exception e) {
            return false; // File is likely still being written
        }
    }
 
    private SendResult sendVideo(File videoFile) {
        System.out.println("Producer " + producerId + " sending " + videoFile.getName() + " (" +
                formatFileSize(videoFile.length()) + ")");
 
        long startTime = System.currentTimeMillis();
        Socket socket = null;
 
        try {
            // Create socket with connection timeout
            socket = new Socket();
            socket.connect(new java.net.InetSocketAddress(serverAddress, serverPort), SOCKET_CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_READ_TIMEOUT_MS);
 
            // Optimize socket for file transfer
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(BUFFER_SIZE * 2);
 
            // Send header
            DataOutputStream headerOut = new DataOutputStream(socket.getOutputStream());
            headerOut.writeInt(producerId);
            headerOut.writeUTF(videoFile.getName());
            headerOut.writeLong(videoFile.length());
            headerOut.flush();
 
            // Get response status
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int statusCode = in.readInt();
            QueueStatus status = QueueStatus.fromCode(statusCode);
 
            if (status == QueueStatus.ACCEPTED) {
                // Send file data with progress tracking
                long totalBytesRead = 0;
                long fileSize = videoFile.length();
                long lastProgressTime = System.currentTimeMillis();
 
                try (FileInputStream fileIn = new FileInputStream(videoFile);
                     BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE)) {
 
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
 
                    while ((bytesRead = fileIn.read(buffer)) != -1 && running.get()) {
                        out.write(buffer, 0, bytesRead);
                        totalBytesRead += bytesRead;
 
                        // Send in chunks to avoid buffer overflow and flush periodically
                        if (totalBytesRead % (BUFFER_SIZE * 8) == 0) {
                            out.flush();
                        }
 
                        // Show progress every 5 seconds
                        long now = System.currentTimeMillis();
                        if (now - lastProgressTime > 5000) {
                            int progress = (int)((totalBytesRead * 100) / fileSize);
                            double speed = (totalBytesRead / 1024.0) / ((now - startTime) / 1000.0);
                            System.out.println("Uploading " + videoFile.getName() + ": " +
                                    progress + "% (" + String.format("%.2f", speed) + " KB/s)");
                            lastProgressTime = now;
                        }
                    }
                    // Final flush to ensure all data is sent
                    out.flush();
 
                    // Wait for transfer completion acknowledgment
                    try {
                        int completionCode = in.readInt();
                        if (completionCode == 100) { // Success code
                            long endTime = System.currentTimeMillis();
                            double transferTime = (endTime - startTime) / 1000.0;
                            System.out.println("Video " + videoFile.getName() +
                                    " uploaded successfully in " + String.format("%.1f", transferTime) +
                                    " seconds (" + String.format("%.2f", fileSize/1024.0/1024.0/transferTime) + " MB/s)");
                            return new SendResult(true, false);
                        } else if (completionCode == QueueStatus.DUPLICATE_HASH.getCode()) {
                            System.out.println("Video " + videoFile.getName() + " is a duplicate (hash match)");
                            return new SendResult(false, true);
                        } else {
                            System.err.println("Server indicated incomplete transfer: code " + completionCode);
                            return new SendResult(false, false);
                        }
                    } catch (IOException e) {
                        System.err.println("No completion acknowledgment received: " + e.getMessage());
                        return new SendResult(false, false);
                    }
                }
            } else {
                // Handle rejection
                String reason = "unknown error";
                boolean isDuplicate = false;
 
                switch (status) {
                    case QUEUE_FULL:
                        reason = "queue full";
                        break;
                    case DUPLICATE_FILE:
                        reason = "duplicate filename";
                        isDuplicate = true;
                        break;
                    case DUPLICATE_HASH:
                        reason = "duplicate content";
                        isDuplicate = true;
                        break;
                    case SERVER_ERROR:
                        reason = "server error";
                        break;
                }
 
                System.out.println("Video " + videoFile.getName() + " rejected: " + reason);
 
                // If queue is full, wait before trying again (only for non-duplicates)
                if (status == QueueStatus.QUEUE_FULL && !isDuplicate) {
                    System.out.println("Waiting 3 seconds before retry...");
                    Thread.sleep(3000);
                }
 
                return new SendResult(false, isDuplicate);
            }
 
        } catch (SocketTimeoutException e) {
            System.err.println("Timeout sending " + videoFile.getName() + ": " + e.getMessage());
            return new SendResult(false, false);
        } catch (IOException e) {
            System.err.println("Error sending " + videoFile.getName() + ": " + e.getMessage());
            return new SendResult(false, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while sending " + videoFile.getName());
            return new SendResult(false, false);
        } finally {
            if (socket != null && !socket.isClosed()) {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.err.println("Error closing socket: " + e.getMessage());
                }
            }
        }
    }
 
    private String formatFileSize(long size) {
        if (size < 1024) {
            return size + " B";
        } else if (size < 1024 * 1024) {
            return String.format("%.2f KB", size / 1024.0);
        } else if (size < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", size / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
        }
    }
 
    // Queue status enum for communication with consumer
    enum QueueStatus {
        ACCEPTED(0),
        QUEUE_FULL(1),
        DUPLICATE_FILE(2),
        DUPLICATE_HASH(3),
        SERVER_ERROR(4);
 
        private final int code;
 
        QueueStatus(int code) {
            this.code = code;
        }
 
        public int getCode() {
            return code;
        }
 
        public static QueueStatus fromCode(int code) {
            for (QueueStatus status : values()) {
                if (status.code == code) return status;
            }
            // Default to server error instead of accepted for unknown codes
            System.err.println("Unknown status code received: " + code);
            return SERVER_ERROR;
        }
    }
}