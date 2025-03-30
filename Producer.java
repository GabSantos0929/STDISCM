import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;

class Producer {
    private ExecutorService producerPool;
    private int producerThreads;
    private String videoFolder = "videos";
    private BlockingQueue<File> queue;
    private String consumerHost = "localhost";
    private int consumerPort = 5000;
    private Set<String> processedFiles = ConcurrentHashMap.newKeySet();

    public Producer(int p, int q) {
        this.producerThreads = p;
        this.producerPool = Executors.newFixedThreadPool(p);
        this.queue = new LinkedBlockingQueue<>(q);
    }

    public void startProducers() {
        for (int i = 1; i <= producerThreads; i++) {
            final String folderPath = videoFolder + "/video" + i;
            producerPool.execute(() -> readFiles(folderPath));
        }
    }

    private boolean isValidVideoFile(String fileName) {
        String[] validFormats = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"};
        String lowerName = fileName.toLowerCase();

        for (String format : validFormats) {
            if (lowerName.endsWith(format)) {
                return true;
            }
        }
        return false;
    }

    private String getFileHash(File file) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] fileBytes = Files.readAllBytes(file.toPath());
            byte[] hashBytes = digest.digest(fileBytes);

            StringBuilder sb = new StringBuilder();
            
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException | IOException e) {
            System.err.println("Error generating hash for file: " + file.getName());
            return null;
        }
    }

    private File compressVideo(File file) {
        try {
            String compressedFilePath = file.getParent() + "/compressed_" + file.getName();
            ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-i", file.getAbsolutePath(), "-vcodec", "libx264", "-crf", "23", compressedFilePath);
            pb.redirectErrorStream(true);

            Process process = pb.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                while ((reader.readLine()) != null);
            }
            process.waitFor();

            File compressedFile = new File(compressedFilePath);
            if (compressedFile.exists()) {
                System.out.println("Compressed video: " + file.getName());
                return compressedFile;
            } else {
                System.out.println("Compression failed, using original file: " + file.getName());
                return file;
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Compression error: " + e.getMessage());
            return file;
        }
    }

    private void uploadToConsumer(File file) {
        try (Socket socket = new Socket(consumerHost, consumerPort);
             FileInputStream fileInputStream = new FileInputStream(file);
             OutputStream outputStream = socket.getOutputStream()) {
            
            outputStream.write((file.getName() + "\n").getBytes());
            outputStream.flush();
            
            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.flush();
            System.out.println("Successfully sent: " + file.getName());
        } catch (IOException e) {
            System.err.println("Failed to send: " + file.getName() + " - " + e.getMessage());
        }
    }

    private void readFiles(String folderPath) {
        File folder = new File(folderPath);
        if (!folder.exists() || !folder.isDirectory()) {
            System.out.println("Folder not found: " + folderPath);
            return;
        }
        
        File[] files = folder.listFiles((dir, name) -> isValidVideoFile(name));
        if (files == null || files.length == 0) {
            System.out.println("No video files found in " + folderPath);
            return;
        }
        
        for (File file : files) {
            try {
                String hash = getFileHash(file);
                if (processedFiles.contains(hash)) {
                    System.out.println("Duplicate video detected, skipping: " + file.getName());
                    continue;
                }
                processedFiles.add(hash);

                File compressedFile = compressVideo(file);

                if (!queue.offer(compressedFile)) {
                    System.out.println("Queue full. Dropping video: " + compressedFile.getName());
                } else {
                    System.out.println("Sending video: " + compressedFile.getName());
                    uploadToConsumer(compressedFile);
                }
            } catch (Exception e) {
                System.err.println("Error processing file: " + file.getName() + " - " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        producerPool.shutdown();
        try {
            if (!producerPool.awaitTermination(10, TimeUnit.MINUTES)) {
                producerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            producerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter number of producer threads (p): ");
        int p = scanner.nextInt();

        System.out.print("Enter number of consumer threads (c): ");
        int c = scanner.nextInt();

        System.out.print("Enter max queue length (q): ");
        int q = scanner.nextInt();

        Producer producerManager = new Producer(p, q);
        producerManager.startProducers();
        producerManager.shutdown();

        scanner.close();
    }
}
