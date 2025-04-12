import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.scene.media.MediaView;
import javafx.stage.Stage;
import javafx.util.Duration;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.PauseTransition;
import javafx.animation.Timeline;
import org.bytedeco.javacv.*;
import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VideoConsumerApp extends Application {
    // Configuration
    private int consumerThreadCount;
    private int queueCapacity;
    private String outputFolder;

    // Concurrent components
    private BlockingQueue<VideoTask> taskQueue;
    private ExecutorService consumerExecutor;
    private ServerSocket serverSocket;
    private volatile boolean running = true;
    private final Map<String, String> fileHashes = new ConcurrentHashMap<>();

    // UI components
    private FlowPane videoGrid;
    private final Map<String, MediaPlayer> previewPlayers = new HashMap<>();

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void init() throws Exception {
        // Parse command line arguments
        Parameters params = getParameters();
        Map<String, String> namedParams = params.getNamed();

        consumerThreadCount = Integer.parseInt(namedParams.getOrDefault("c", "1"));
        queueCapacity = Integer.parseInt(namedParams.getOrDefault("q", "1"));
        outputFolder = namedParams.getOrDefault("output", "videos");

        // Initialize components
        taskQueue = new LinkedBlockingQueue<>(queueCapacity);
        consumerExecutor = Executors.newFixedThreadPool(consumerThreadCount);

        // Create output directory
        new File(outputFolder).mkdirs();

        // Start server socket and consumer threads
        startNetworkServer();
        startConsumerThreads();
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Video Upload Service");

        // Create UI layout
        BorderPane mainLayout = new BorderPane();

        // Status panel at top
        HBox statusPanel = new HBox(10);
        Label queueStatus = new Label("Queue: 0/" + queueCapacity);
        Label threadsStatus = new Label("Active consumers: " + consumerThreadCount);
        statusPanel.getChildren().addAll(new Label("Status:"), queueStatus, threadsStatus);
        statusPanel.setPadding(new Insets(10));
        mainLayout.setTop(statusPanel);

        // Video grid in center
        videoGrid = new FlowPane();
        videoGrid.setPadding(new Insets(10));
        videoGrid.setHgap(10);
        videoGrid.setVgap(10);
        ScrollPane scrollPane = new ScrollPane(videoGrid);
        scrollPane.setFitToWidth(true);
        mainLayout.setCenter(scrollPane);

        // Initial population of video grid
        updateVideoGrid();

        // Setup periodic UI updates
        setupPeriodicUpdates(queueStatus, threadsStatus);

        // Set scene and show
        Scene scene = new Scene(mainLayout, 800, 600);
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private void setupPeriodicUpdates(final Label queueStatus, final Label threadsStatus) {
        Timeline timeline = new Timeline(
                new KeyFrame(Duration.seconds(1), event -> {
                    // Update queue status
                    queueStatus.setText("Queue: " + taskQueue.size() + "/" + queueCapacity);

                    // Update thread status - count active threads
                    int activeCount = 0;
                    if (consumerExecutor instanceof ThreadPoolExecutor) {
                        ThreadPoolExecutor executor = (ThreadPoolExecutor) consumerExecutor;
                        activeCount = executor.getActiveCount();
                    }
                    threadsStatus.setText("Active consumers: " + activeCount + "/" + consumerThreadCount);

                    // Check for new videos
                    updateVideoGrid();
                })
        );
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();
    }
    private void updateVideoGrid() {
        Platform.runLater(() -> {
            File folder = new File(outputFolder);
            File[] videoFiles = folder.listFiles((dir, name) -> {
                String lowerName = name.toLowerCase();
                return !name.startsWith("temp_") && !name.startsWith("compressed_") &&
                        (lowerName.endsWith(".mp4") || lowerName.endsWith(".avi") ||
                                lowerName.endsWith(".mkv") || lowerName.endsWith(".mov") ||
                                lowerName.endsWith(".wmv") || lowerName.endsWith(".flv") ||
                                lowerName.endsWith(".webm") || lowerName.endsWith(".m4v"));
            });

            if (videoFiles == null) return;

            // Find new videos that aren't already in the grid (now inside Platform.runLater)
            Set<String> existingVideos = new HashSet<>();
            for (Node node : videoGrid.getChildren()) {
                if (node instanceof VBox) {
                    VBox box = (VBox) node;
                    for (Node child : box.getChildren()) {
                        if (child instanceof Label) {
                            existingVideos.add(((Label) child).getText());
                            break;
                        }
                    }
                }
            }

            // Add new videos to grid (now inside Platform.runLater)
            for (File videoFile : videoFiles) {
                if (!existingVideos.contains(videoFile.getName())) {
                    videoGrid.getChildren().add(createVideoThumbnail(videoFile));
                }
            }

            // Also update progress for videos in processing (now inside Platform.runLater)
            for (Node node : videoGrid.getChildren()) {
                if (node instanceof VBox) {
                    VBox box = (VBox) node;
                    String fileName = null;

                    // Find the label with the filename
                    for (Node child : box.getChildren()) {
                        if (child instanceof Label) {
                            fileName = ((Label) child).getText();
                            break;
                        }
                    }

                    if (fileName != null) {
                        // Create a thread-safe copy of the task queue to avoid concurrent modification
                        List<VideoTask> tasks = new ArrayList<>(taskQueue);

                        // Find corresponding task
                        final String finalFileName = fileName;
                        for (VideoTask task : tasks) {
                            if (task.finalFile.getName().equals(finalFileName) &&
                                    task.state == VideoTask.ProcessingState.COMPRESSING) {

                                // Update the progress bar
                                for (Node child : box.getChildren()) {
                                    if (child instanceof ProgressBar) {
                                        ProgressBar progressBar = (ProgressBar) child;
                                        // Get a snapshot of the progress value
                                        double currentProgress = task.progress;
                                        progressBar.setProgress(currentProgress);
                                        progressBar.setVisible(true);
                                        break;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });
    }


    private VBox createVideoThumbnail(File videoFile) {
        // Create media player for this video
        Media media = new Media(videoFile.toURI().toString());
        MediaPlayer player = new MediaPlayer(media);
        previewPlayers.put(videoFile.getName(), player);

        // Create video view
        MediaView mediaView = new MediaView(player);
        mediaView.setFitWidth(160);
        mediaView.setFitHeight(120);

        // Get thumbnail from video
        player.setOnReady(() -> {
            player.seek(Duration.seconds(1));
            player.play();
            player.setMute(true);
            PauseTransition pause = new PauseTransition(Duration.millis(100));
            pause.setOnFinished(e -> player.pause());
            pause.play();
        });

        // Create container
        VBox videoBox = new VBox(5);
        videoBox.setPadding(new Insets(5));
        videoBox.setStyle("-fx-border-color: #cccccc; -fx-border-width: 1;");

        Label titleLabel = new Label(videoFile.getName());
        titleLabel.setMaxWidth(160);
        titleLabel.setAlignment(Pos.CENTER);

        // Add progress bar (initially hidden)
        ProgressBar progressBar = new ProgressBar(0);
        progressBar.setPrefWidth(160);
        progressBar.setVisible(false);

        videoBox.getChildren().addAll(mediaView, titleLabel, progressBar);

        // Hover behavior - preview 10 seconds
        videoBox.setOnMouseEntered(e -> {
            player.seek(Duration.ZERO);
            player.setMute(false);
            player.play();

            // Stop after 10 seconds
            PauseTransition pause = new PauseTransition(Duration.seconds(10));
            pause.setOnFinished(pauseEvent -> player.pause());
            pause.play();
        });

        videoBox.setOnMouseExited(e -> {
            player.pause();
            player.seek(Duration.seconds(1));
        });

        // Click action - open full player
        videoBox.setOnMouseClicked(e -> openVideoPlayer(videoFile));

        return videoBox;
    }

    public void openVideoPlayer(File videoFile) {
        Stage playerStage = new Stage();
        playerStage.setTitle("Playing: " + videoFile.getName());

        Media media = new Media(videoFile.toURI().toString());
        MediaPlayer player = new MediaPlayer(media);
        MediaView mediaView = new MediaView(player);
        mediaView.setPreserveRatio(true);

        // Use a StackPane for the MediaView. This helps with centering if preserveRatio is true.
        StackPane mediaViewPane = new StackPane(mediaView);
        mediaViewPane.setStyle("-fx-background-color: black;"); // Background for letter/pillarboxing

        // --- Controls ---
        HBox controls = new HBox(10);
        controls.setPadding(new Insets(10));
        controls.setAlignment(Pos.CENTER);
        controls.setPrefHeight(40); // Use prefHeight for BorderPane bottom
        controls.setStyle("-fx-background-color: #f0f0f0;");

        Button playButton = new Button("Play");
        Button pauseButton = new Button("Pause");
        Button stopButton = new Button("Stop");
        Button fullscreenButton = new Button("Fullscreen");

        playButton.setOnAction(e -> player.play());
        pauseButton.setOnAction(e -> player.pause());
        stopButton.setOnAction(e -> {
            player.stop();
            playerStage.close();
        });
        fullscreenButton.setOnAction(e -> {
            // Set fullscreen hint *before* changing state - might help on some systems
            // Not strictly necessary but sometimes helps rendering pipelines
            mediaView.setSmooth(false); // Temporarily disable smoothing during transition
            playerStage.setFullScreen(!playerStage.isFullScreen());
        });

        controls.getChildren().addAll(playButton, pauseButton, stopButton, fullscreenButton);

        // --- Layout ---
        BorderPane root = new BorderPane();
        root.setCenter(mediaViewPane);
        root.setBottom(controls);

        // --- Scene ---
        // Create scene *before* bindings that might depend on scene properties
        Scene scene = new Scene(root, 640, 480);
        playerStage.setScene(scene);

        // --- Bindings (Crucial Part) ---
        // Bind the container pane's size to the available space in the root pane.
        // BorderPane automatically manages the size of its center node.
        // So, bind the MediaView's fit properties DIRECTLY to the container pane's size.
        mediaView.fitWidthProperty().bind(mediaViewPane.widthProperty());
        mediaView.fitHeightProperty().bind(mediaViewPane.heightProperty());


        // --- Fullscreen Listener (Revised Logic) ---
        playerStage.fullScreenProperty().addListener((obs, oldVal, newVal) -> {
            // No matter entering or exiting, ensure controls are managed correctly
            root.setBottom(newVal ? null : controls); // Hide controls in FS, show otherwise

            Platform.runLater(() -> {
                // This seems counter-intuitive, but sometimes unbinding and rebinding
                // forces the MediaView to re-evaluate dimensions properly after
                // the stage/scene finishes its internal resize logic.

                // 1. Unbind temporarily
                mediaView.fitWidthProperty().unbind();
                mediaView.fitHeightProperty().unbind();

                // Optional: Add a tiny pause/play cycle - can force redraw sometimes
                // Status currentStatus = player.getStatus();
                // if (currentStatus == Status.PLAYING) {
                //    player.pause();
                //    player.play();
                // } else if (currentStatus == Status.PAUSED) {
                //     // If paused, maybe don't force play, just ensure state.
                // }


                // 2. Rebind after a very short delay (let layout settle)
                // We are already inside Platform.runLater, another one might be overkill,
                // but let's try ensuring the layout pass happened.
                Platform.runLater(() -> {
                    mediaView.fitWidthProperty().bind(mediaViewPane.widthProperty());
                    mediaView.fitHeightProperty().bind(mediaViewPane.heightProperty());
                    root.requestLayout(); // Ask root to relayout NOW
                    mediaView.setSmooth(true); // Re-enable smoothing
                });


                // Old approach fallback (if unbind/rebind fails):
                // Just requesting layout might be enough if bindings are correct
                // root.requestLayout();
                // mediaViewPane.requestLayout();
            });
        });


        // --- Final Setup ---
        playerStage.setOnCloseRequest(e -> {
            player.stop();
        });

        playerStage.show();
        player.play();
    }

    private void startNetworkServer() {
        new Thread(() -> {
            try {
                serverSocket = new ServerSocket(8080, 0, InetAddress.getByName("0.0.0.0"));
                System.out.println("Consumer server started on port 8080");

                while (running) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("New producer connected from " + clientSocket.getInetAddress());
                    new Thread(() -> handleClientConnection(clientSocket)).start();
                }
            } catch (IOException e) {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void handleClientConnection(Socket clientSocket) {
        File tempFile = null;
        String filename = null;

        try {
            DataInputStream in = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

            // Read header
            int producerId = in.readInt();
            filename = in.readUTF();
            long fileSize = in.readLong();

            System.out.println("Receiving " + filename + " (" + fileSize + " bytes) from producer " + producerId);

            // First, perform all file system and queue checks while holding a lock to prevent race conditions
            synchronized (fileHashes) {
                // Check queue capacity
                if (taskQueue.size() >= queueCapacity) {
                    out.writeInt(QueueStatus.QUEUE_FULL.getCode());
                    System.out.println("Rejected " + filename + " - QUEUE_FULL");
                    return;
                }

                // Check for file with same name
                File existingFile = new File(outputFolder, filename);
                if (existingFile.exists()) {
                    out.writeInt(QueueStatus.DUPLICATE_FILE.getCode());
                    System.out.println("Rejected " + filename + " - DUPLICATE_FILE");
                    return;
                }

                // Tell producer it's OK to send file
                out.writeInt(QueueStatus.ACCEPTED.getCode());
            }

            // Create unique temp file with producer ID to avoid collisions
            tempFile = new File(outputFolder, "temp_" + producerId + "_" + filename);

            // Read file data
            try (FileOutputStream fileOut = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                long bytesRemaining = fileSize;

                while (bytesRemaining > 0) {
                    int bytesToRead = (int) Math.min(buffer.length, bytesRemaining);
                    int bytesRead = in.read(buffer, 0, bytesToRead);
                    if (bytesRead == -1) {
                        throw new IOException("Unexpected end of stream");
                    }
                    fileOut.write(buffer, 0, bytesRead);
                    bytesRemaining -= bytesRead;
                }
            }

            // Calculate hash of received file
            String fileHash = calculateFileHash(tempFile);

            // Synchronize the hash check and queue addition to prevent race conditions
            synchronized (fileHashes) {
                // Check for file with same hash
                boolean isDuplicate = false;
                String duplicateFilename = null;

                for (Map.Entry<String, String> entry : fileHashes.entrySet()) {
                    if (entry.getValue().equals(fileHash)) {
                        isDuplicate = true;
                        duplicateFilename = entry.getKey();
                        break;
                    }
                }

                if (isDuplicate) {
                    // Delete temp file if duplicate content
                    if (tempFile != null && tempFile.exists()) {
                        tempFile.delete();
                    }
                    System.out.println("Hash duplicate detected: " + filename + " is duplicate of " + duplicateFilename);
                    out.writeInt(QueueStatus.DUPLICATE_HASH.getCode());
                    return;
                }

                // Store hash and add to queue
                fileHashes.put(filename, fileHash);

                // Create final file object
                File finalFile = new File(outputFolder, filename);

                // Make sure we can offer to the queue before proceeding
                if (!taskQueue.offer(new VideoTask(tempFile, finalFile))) {
                    // Queue became full while we were processing
                    if (tempFile != null && tempFile.exists()) {
                        tempFile.delete();
                    }
                    fileHashes.remove(filename);
                    out.writeInt(QueueStatus.QUEUE_FULL.getCode());
                    System.out.println("Queue filled during processing: " + filename);
                    return;
                }

                System.out.println("Added " + filename + " to processing queue");
            }

            // Send completion acknowledgment
            out.writeInt(100);

        } catch (IOException e) {
            e.printStackTrace();
            // Clean up on error
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
            if (filename != null) {
                synchronized (fileHashes) {
                    fileHashes.remove(filename);
                }
            }
        } finally {
            try {
                clientSocket.setSoTimeout(5000);
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
//    private void handleClientConnection(Socket clientSocket) {
//        try {
//            DataInputStream in = new DataInputStream(clientSocket.getInputStream());
//            DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
//
//            // Read header
//            int producerId = in.readInt();
//            String filename = in.readUTF();
//            long fileSize = in.readLong();
//
//            System.out.println("Receiving " + filename + " (" + fileSize + " bytes) from producer " + producerId);
//
//            // Enhanced queue status response
//            QueueStatus status;
//
//            // Check available space in queue
//            if (taskQueue.size() >= queueCapacity) {
//                status = QueueStatus.QUEUE_FULL;
//            } else {
//                // Check if file with same name exists
//                File existingFile = new File(outputFolder, filename);
//                if (existingFile.exists()) {
//                    status = QueueStatus.DUPLICATE_FILE;
//                } else {
//                    status = QueueStatus.ACCEPTED;
//                }
//            }
//
//            // Send response to producer BEFORE trying to read file data
//            out.writeInt(status.getCode());
//
//            if (status != QueueStatus.ACCEPTED) {
//                System.out.println("Rejected " + filename + " - " + status);
//                return; // Exit early if rejected
//            }
//
//            // Only read file data if we've accepted the file
//            File tempFile = new File(outputFolder, "temp_" + filename);
//
//            try (FileOutputStream fileOut = new FileOutputStream(tempFile)) {
//                // Read file data now that we've sent acceptance
//                byte[] buffer = new byte[8192];
//                long bytesRemaining = fileSize;
//
//                while (bytesRemaining > 0) {
//                    int bytesToRead = (int) Math.min(buffer.length, bytesRemaining);
//                    int bytesRead = in.read(buffer, 0, bytesToRead);
//                    if (bytesRead == -1) {
//                        throw new IOException("Unexpected end of stream");
//                    }
//                    fileOut.write(buffer, 0, bytesRead);
//                    bytesRemaining -= bytesRead;
//                }
//            }
//
//            // Calculate hash after receiving the file
//            String fileHash = calculateFileHash(tempFile);
//
//            // Check if file with same hash exists
//            boolean isDuplicate = false;
//            String duplicateFilename = null;
//
//            for (Map.Entry<String, String> entry : fileHashes.entrySet()) {
//                if (entry.getValue().equals(fileHash)) {
//                    isDuplicate = true;
//                    duplicateFilename = entry.getKey();
//                    break;
//                }
//            }
//
//            if (isDuplicate) {
//                // File with same content already exists
//                tempFile.delete();
//                System.out.println("Hash duplicate detected: " + filename + " is duplicate of " + duplicateFilename);
//
//                // Send rejection due to duplicate hash
//                out.writeInt(QueueStatus.DUPLICATE_HASH.getCode());
//                return;
//            }
//
//            // Not a duplicate, store the hash for future checks
//            fileHashes.put(filename, fileHash);
//
//            // Add to processing queue
//            taskQueue.offer(new VideoTask(tempFile, new File(outputFolder, filename)));
//            System.out.println("Added " + filename + " to processing queue");
//
//            // Send completion acknowledgment to producer
//            out.writeInt(100);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                clientSocket.setSoTimeout(5000);
//                clientSocket.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    // New helper method to calculate hash of the whole file
    private String calculateFileHash(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
            byte[] hash = digest.digest();
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    private void startConsumerThreads() {
        for (int i = 0; i < consumerThreadCount; i++) {
            final int consumerId = i;
            consumerExecutor.submit(() -> {
                System.out.println("Consumer thread " + consumerId + " started");
                while (running) {
                    try {
                        VideoTask task = taskQueue.take();
                        processVideo(task, consumerId);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }
    }

    private void processVideo(VideoTask task, int consumerId) {
        try {
            System.out.println("Consumer " + consumerId + " processing " + task.finalFile.getName());

            // Compress video if it's over a certain size
            if (task.tempFile.length() > 0.1 * 1024 * 1024) { // 5MB
                task.state = VideoTask.ProcessingState.COMPRESSING;
                task.progress = 0.0;

                // Update UI to show compression progress
                Platform.runLater(this::updateVideoGrid);

                // Perform actual compression
                compressVideo(task);
            } else {
                // Fixed file move operation - use NIO Files API instead of renameTo
                try {
                    Files.move(task.tempFile.toPath(), task.finalFile.toPath(),
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    System.out.println("Failed to move " + task.tempFile.getName() + ": " + e.getMessage());
                    // Try the copy and delete approach as fallback
                    Files.copy(task.tempFile.toPath(), task.finalFile.toPath(),
                            StandardCopyOption.REPLACE_EXISTING);
                    Files.delete(task.tempFile.toPath());
                }
            }

            task.state = VideoTask.ProcessingState.COMPLETE;
            task.progress = 1.0;

            System.out.println("Video " + task.finalFile.getName() + " ready");

            // Notify UI thread of new video
            Platform.runLater(this::updateVideoGrid);

        } catch (Exception e) {
            e.printStackTrace();
            // Cleanup on error
            if (task.tempFile.exists()) {
                task.tempFile.delete();
            }
            synchronized (fileHashes) {
                fileHashes.remove(task.finalFile.getName());
            }
        }
    }

    private void compressVideo(VideoTask task) throws Exception {
        System.out.println("COMPRESSION START: " + task.tempFile.getName());

        // Create temporary output file path
        File tempOutputFile = new File(task.finalFile.getParentFile(), "compressed_" + task.finalFile.getName());
        String compressedFilePath = tempOutputFile.getAbsolutePath();
        System.out.println("OUTPUT FILE: " + compressedFilePath);

        // Initialize grabber
        System.out.println("CREATING GRABBER...");
        FFmpegFrameGrabber grabber = null;
        try {
            grabber = new FFmpegFrameGrabber(task.tempFile);
            System.out.println("STARTING GRABBER...");
            grabber.start();
            System.out.println("GRABBER STARTED SUCCESSFULLY");
        } catch (Exception e) {
            System.err.println("GRABBER FAILED: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // Get video metadata
        System.out.println("READING VIDEO METADATA...");
        int width = grabber.getImageWidth();
        int height = grabber.getImageHeight();
        double frameRate = grabber.getVideoFrameRate();
        int totalFrames = grabber.getLengthInFrames();
        System.out.println("VIDEO INFO: " + width + "x" + height + ", " + frameRate + " fps, " + totalFrames + " frames");

        // Initialize recorder
        System.out.println("CREATING RECORDER...");
        FFmpegFrameRecorder recorder = null;
        try {
            recorder = new FFmpegFrameRecorder(compressedFilePath, width, height);
            recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
            recorder.setFormat("mp4");
            recorder.setFrameRate(frameRate);
            recorder.setVideoBitrate(1000000);

            // Configure audio if present
            if (grabber.getAudioChannels() > 0) {
                System.out.println("CONFIGURING AUDIO: " + grabber.getAudioChannels() + " channels");
                recorder.setAudioChannels(grabber.getAudioChannels());
                recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
                recorder.setSampleRate(grabber.getSampleRate());
                recorder.setAudioBitrate(128000);
            }

            System.out.println("STARTING RECORDER...");
            recorder.start();
            System.out.println("RECORDER STARTED SUCCESSFULLY");
        } catch (Exception e) {
            System.err.println("RECORDER FAILED: " + e.getMessage());
            e.printStackTrace();
            if (grabber != null) {
                try { grabber.stop(); } catch (Exception ex) { /* ignore */ }
                try { grabber.release(); } catch (Exception ex) { /* ignore */ }
            }
            throw e;
        }

        // Process frame by frame
        System.out.println("BEGINNING FRAME PROCESSING...");
        Frame frame = null;
        int frameCount = 0;

        try {
            while ((frame = grabber.grab()) != null) {
                // Print status every 100 frames
                if (frameCount % 100 == 0) {
                    System.out.println("PROCESSED " + frameCount + " FRAMES");
                }

                // Try recording the frame
                recorder.record(frame);

                // Update progress
                frameCount++;
                if (totalFrames > 0) {
                    task.progress = Math.min(0.95, (double)frameCount / totalFrames);
                }
            }
            System.out.println("ALL FRAMES PROCESSED: " + frameCount + " total frames");
        } catch (Exception e) {
            System.err.println("FRAME PROCESSING FAILED AT FRAME " + frameCount + ": " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            // Close resources
            System.out.println("STOPPING RECORDER...");
            try {
                if (recorder != null) {
                    recorder.stop();
                    recorder.release();
                    System.out.println("RECORDER STOPPED");
                }
            } catch (Exception e) {
                System.err.println("ERROR STOPPING RECORDER: " + e.getMessage());
            }

            System.out.println("STOPPING GRABBER...");
            try {
                if (grabber != null) {
                    grabber.stop();
                    grabber.release();
                    System.out.println("GRABBER STOPPED");
                }
            } catch (Exception e) {
                System.err.println("ERROR STOPPING GRABBER: " + e.getMessage());
            }
        }

        // Replace the original file with the compressed one
        System.out.println("FINALIZING OUTPUT FILE...");
        task.tempFile.delete();
        boolean renamed = tempOutputFile.renameTo(task.finalFile);
        System.out.println("FILE RENAME SUCCESS: " + renamed);

        if (!renamed) {
            // If rename fails, try copying content
            System.out.println("RENAME FAILED, TRYING COPY...");
            Files.copy(tempOutputFile.toPath(), task.finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempOutputFile.delete();
            System.out.println("COPY SUCCESSFUL");
        }

        task.progress = 1.0;
        System.out.println("COMPRESSION COMPLETE: " + task.finalFile.getName());
    }
//    private void compressVideo(VideoTask task) throws Exception {
//        System.out.println("Compressing " + task.tempFile.getName());
//
//        // Create temporary output file path
//        File tempOutputFile = new File(task.finalFile.getParentFile(), "compressed_" + task.finalFile.getName());
//        String compressedFilePath = tempOutputFile.getAbsolutePath();
//
//        // Initialize grabber to read input video
//        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(task.tempFile);
//        grabber.start();
//
//        // Get video metadata
//        int width = grabber.getImageWidth();
//        int height = grabber.getImageHeight();
//        double frameRate = grabber.getVideoFrameRate();
//        int totalFrames = grabber.getLengthInFrames();
//
//        // Initialize recorder for output video
//        FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(compressedFilePath, width, height);
//        recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
//        recorder.setFormat("mp4");
//        recorder.setFrameRate(frameRate);
//        recorder.setVideoBitrate(1000000); // 1 Mbps
//
//        // Configure audio if present
//        if (grabber.getAudioChannels() > 0) {
//            recorder.setAudioChannels(grabber.getAudioChannels());
//            recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
//            recorder.setSampleRate(grabber.getSampleRate());
//            recorder.setAudioBitrate(128000); // 128 kbps
//        }
//
//        recorder.start();
//
//        // Process frame by frame
//        Frame frame;
//        int frameCount = 0;
//
//        while ((frame = grabber.grab()) != null) {
//            // Write frame to output
//            recorder.record(frame);
//
//            // Update progress (0.0 to 0.95 to leave room for file operations)
//            frameCount++;
//            if (totalFrames > 0) { // Avoid division by zero
//                task.progress = Math.min(0.95, (double)frameCount / totalFrames);
//
//                // Update UI every 30 frames to avoid overloading JavaFX
//                if (frameCount % 30 == 0) {
//                    Platform.runLater(this::updateVideoGrid);
//                }
//            }
//        }
//
//        // Close resources
//        recorder.stop();
//        recorder.release();
//        grabber.stop();
//        grabber.release();
//
//        // Replace the original file with the compressed one
//        task.tempFile.delete();
//        if (!tempOutputFile.renameTo(task.finalFile)) {
//            // If rename fails, try copying content
//            Files.copy(tempOutputFile.toPath(), task.finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
//            tempOutputFile.delete();
//        }
//
//        task.progress = 1.0;
//        System.out.println("Compression complete: " + task.finalFile.getName());
//    }
//
//    private long getDurationInSeconds(File videoFile) {
//        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(videoFile)) {
//            grabber.start();
//            long microseconds = grabber.getLengthInTime();
//            // Convert microseconds to seconds
//            return microseconds / 1000000;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return 0;
//        }
//    }


    @Override
    public void stop() {
        // Shutdown
        running = false;

        // Close server socket
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Shutdown consumer threads
        if (consumerExecutor != null) {
            consumerExecutor.shutdownNow();
        }

        // Dispose media players
        for (MediaPlayer player : previewPlayers.values()) {
            player.dispose();
        }
    }

    // Queue status enum for communication with producer
    // Update the QueueStatus enum to include DUPLICATE_HASH
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
            return ACCEPTED; // Default
        }
    }

    // Video task class
    static class VideoTask {
        final File tempFile;
        final File finalFile;
        enum ProcessingState {
            WAITING,
            COMPRESSING,
            COMPLETE
        }

        volatile ProcessingState state = ProcessingState.WAITING;
        volatile double progress = 0.0;

        VideoTask(File tempFile, File finalFile) {
            this.tempFile = tempFile;
            this.finalFile = finalFile;
        }
    }

}