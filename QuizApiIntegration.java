import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Production-Quality Quiz API Integration Program
 * 
 * Fetches quiz data from 10 polls, deduplicates events, aggregates scores,
 * builds a leaderboard, and submits results via API.
 * 
 * Requirements:
 * - Java 11+
 * - Jackson databind (add to classpath or Maven)
 */
public class QuizApiIntegration {

    // ==================== Configuration ====================
    private static final String BASE_URL = "https://devapigw.vidalhealthtpa.com/srm-quiz-task/quiz";
    private static final String REG_NO = "RA2311003012260";
    private static final int TOTAL_POLLS = 10;
    private static final int DELAY_BETWEEN_CALLS_MS = 5000; // 5 seconds
    private static final int HTTP_TIMEOUT_SECONDS = 30;

    // ==================== Data Classes ====================

    /**
     * Represents a single quiz event with roundId, participant, and score.
     * Used for JSON deserialization and internal processing.
     */
    static class Event {
        @JsonProperty("roundId")
        public String roundId;

        @JsonProperty("participant")
        public String participant;

        @JsonProperty("score")
        public Integer score;

        public Event() {
        }

        public Event(String roundId, String participant, Integer score) {
            this.roundId = roundId;
            this.participant = participant;
            this.score = score;
        }

        @Override
        public String toString() {
            return String.format("{roundId='%s', participant='%s', score=%d}",
                    roundId, participant, score);
        }

        /**
         * Generate deduplication key: roundId_participant
         * This ensures each participant's score per round is counted only once.
         */
        public String getDeduplicationKey() {
            return roundId + "_" + participant;
        }
    }

    /**
     * Represents the API response structure for a single poll.
     * Contains regNo, pollIndex, and list of events.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ApiResponse {
        @JsonProperty("regNo")
        public String regNo;

        @JsonProperty("pollIndex")
        public Integer pollIndex;

        @JsonProperty("events")
        public List<Event> events;

        public ApiResponse() {
            this.events = new ArrayList<>();
        }

        @Override
        public String toString() {
            return String.format("ApiResponse{regNo='%s', pollIndex=%d, events=%d}",
                    regNo, pollIndex, events != null ? events.size() : 0);
        }
    }

    /**
     * Represents a leaderboard entry with participant name and total score.
     * Used for final output and sorting.
     */
    static class LeaderboardEntry implements Comparable<LeaderboardEntry> {
        public String participant;
        public Integer totalScore;

        public LeaderboardEntry(String participant, Integer totalScore) {
            this.participant = participant;
            this.totalScore = totalScore;
        }

        /**
         * Compare by totalScore in descending order (higher scores first).
         */
        @Override
        public int compareTo(LeaderboardEntry other) {
            return other.totalScore.compareTo(this.totalScore);
        }

        @Override
        public String toString() {
            return String.format("%-20s : %d", participant, totalScore);
        }
    }

    // ==================== State Management ====================
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Set<String> seenEventKeys; // For deduplication
    private final Map<String, Integer> participantScores; // For aggregation
    private final List<Event> allEvents; // Store all events for debugging

    // ==================== Constructor ====================
    public QuizApiIntegration() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                .build();
        this.objectMapper = new ObjectMapper();
        this.seenEventKeys = new HashSet<>();
        this.participantScores = new HashMap<>();
        this.allEvents = new ArrayList<>();
        log("✓ QuizApiIntegration initialized");
    }

    // ==================== Main Workflow ====================

    /**
     * Main entry point: orchestrates the entire workflow.
     * 1. Fetch data from all 10 polls
     * 2. Process and deduplicate events
     * 3. Build leaderboard
     * 4. Submit results via POST API
     */
    public void execute() {
        try {
            log("\n" + "=".repeat(70));
            log("STARTING QUIZ API INTEGRATION WORKFLOW");
            log("=".repeat(70));

            // Step 1: Fetch poll data
            List<ApiResponse> pollResponses = fetchPollData();
            if (pollResponses.isEmpty()) {
                logError("No poll data fetched. Aborting workflow.");
                return;
            }

            // Step 2: Process events (deduplicate and aggregate)
            processEvents(pollResponses);

            // Step 3: Build and display leaderboard
            List<LeaderboardEntry> leaderboard = buildLeaderboard();
            displayLeaderboard(leaderboard);

            // Step 4: Calculate total score
            Integer totalScore = calculateTotalScore();
            log("\n[TOTAL SCORE ACROSS ALL PARTICIPANTS]: " + totalScore);

            // Step 5: Submit results via POST API
            submitResult(leaderboard, totalScore);

            log("\n" + "=".repeat(70));
            log("WORKFLOW COMPLETED SUCCESSFULLY");
            log("=".repeat(70) + "\n");

        } catch (Exception e) {
            logError("Fatal error during workflow: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ==================== Step 1: Fetch Poll Data ====================

    /**
     * Fetches quiz data from all 10 polls with 5-second delay between calls.
     * 
     * @return List of ApiResponse objects from all polls
     */
    private List<ApiResponse> fetchPollData() {
        List<ApiResponse> responses = new ArrayList<>();
        log("\n[STEP 1] FETCHING POLL DATA FROM 10 POLLS...");
        log("-".repeat(70));

        for (int poll = 0; poll < TOTAL_POLLS; poll++) {
            try {
                // Respect 5-second delay between calls
                if (poll > 0) {
                    log(String.format("⏳ Waiting 5 seconds before poll %d...", poll));
                    Thread.sleep(DELAY_BETWEEN_CALLS_MS);
                }

                ApiResponse response = fetchSinglePoll(poll);
                if (response != null) {
                    responses.add(response);
                    log(String.format("✓ Poll %d fetched: %s", poll, response));
                }

            } catch (InterruptedException e) {
                logError("Thread interrupted while fetching poll " + poll);
                Thread.currentThread().interrupt();
                break;
            }
        }

        log(String.format("\n✓ Fetched %d/%d polls successfully", responses.size(), TOTAL_POLLS));
        return responses;
    }

    /**
     * Fetches data for a single poll with retry logic.
     * Retries once if the initial request fails.
     * 
     * @param pollIndex Poll number (0-9)
     * @return ApiResponse or null if failed after retries
     */
    private ApiResponse fetchSinglePoll(int pollIndex) {
        String url = BASE_URL + "/messages?regNo=" + REG_NO + "&poll=" + pollIndex;
        int maxRetries = 2;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log(String.format("  → Poll %d, Attempt %d: GET %s", pollIndex, attempt, url));

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request,
                        HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    log(String.format("  ✓ Response received (status: %d)", response.statusCode()));

                    // Parse JSON response
                    ApiResponse apiResponse = objectMapper.readValue(
                            response.body(), ApiResponse.class);

                    // Validation
                    if (apiResponse == null) {
                        logError("API response is null for poll " + pollIndex);
                        continue;
                    }

                    if (apiResponse.events == null || apiResponse.events.isEmpty()) {
                        log(String.format("  ⚠ Poll %d has no events", pollIndex));
                        apiResponse.events = new ArrayList<>();
                    } else {
                        log(String.format("  ℹ Poll %d contains %d events",
                                pollIndex, apiResponse.events.size()));
                    }

                    return apiResponse;

                } else {
                    logError(String.format("Poll %d returned status %d",
                            pollIndex, response.statusCode()));
                }

            } catch (IOException e) {
                logError(String.format("Network error on poll %d, attempt %d: %s",
                        pollIndex, attempt, e.getMessage()));
            } catch (InterruptedException e) {
                logError(String.format("Interrupted while fetching poll %d", pollIndex));
                Thread.currentThread().interrupt();
                break;
            }
        }

        logError("Failed to fetch poll " + pollIndex + " after " + maxRetries + " attempts");
        return null;
    }

    // ==================== Step 2: Process Events ====================

    /**
     * Processes all events from poll responses:
     * - Deduplicates events using HashSet (key: roundId_participant)
     * - Aggregates scores using Map<String, Integer>
     * 
     * @param pollResponses List of API responses from all polls
     */
    private void processEvents(List<ApiResponse> pollResponses) {
        log("\n[STEP 2] PROCESSING EVENTS (DEDUPLICATION & AGGREGATION)...");
        log("-".repeat(70));

        for (ApiResponse response : pollResponses) {
            if (response.events == null || response.events.isEmpty()) {
                continue;
            }

            for (Event event : response.events) {
                // Validation: Ensure event has required fields
                if (event.roundId == null || event.participant == null || event.score == null) {
                    logError("Skipping invalid event: " + event);
                    continue;
                }

                String deduplicationKey = event.getDeduplicationKey();

                // Deduplication: Check if we've already seen this event
                if (seenEventKeys.contains(deduplicationKey)) {
                    log(String.format("  ⚠ Duplicate event ignored: %s", deduplicationKey));
                    continue;
                }

                // Mark as seen
                seenEventKeys.add(deduplicationKey);
                allEvents.add(event);

                // Aggregation: Add score to participant's total
                participantScores.merge(event.participant, event.score, Integer::sum);

                log(String.format("  ✓ Event processed: %s -> Score: %d",
                        deduplicationKey, event.score));
            }
        }

        log(String.format("\n✓ Total events processed: %d", allEvents.size()));
        log(String.format("✓ Total participants: %d", participantScores.size()));
        log(String.format("✓ Deduplication key set size: %d", seenEventKeys.size()));
    }

    // ==================== Step 3: Build Leaderboard ====================

    /**
     * Builds leaderboard sorted by totalScore in descending order.
     * 
     * @return List of LeaderboardEntry sorted by score (highest first)
     */
    private List<LeaderboardEntry> buildLeaderboard() {
        log("\n[STEP 3] BUILDING LEADERBOARD...");
        log("-".repeat(70));

        List<LeaderboardEntry> leaderboard = participantScores.entrySet().stream()
                .map(entry -> new LeaderboardEntry(entry.getKey(), entry.getValue()))
                .sorted()
                .collect(Collectors.toList());

        log(String.format("✓ Leaderboard built with %d entries", leaderboard.size()));
        return leaderboard;
    }

    /**
     * Displays leaderboard in a formatted table for console output.
     * 
     * @param leaderboard List of LeaderboardEntry objects
     */
    private void displayLeaderboard(List<LeaderboardEntry> leaderboard) {
        log("\n[LEADERBOARD - SORTED BY SCORE]");
        log("-".repeat(70));

        if (leaderboard.isEmpty()) {
            log("⚠ Leaderboard is empty");
            return;
        }

        log(String.format("%-20s : %s", "PARTICIPANT", "TOTAL SCORE"));
        log("-".repeat(70));

        for (int i = 0; i < leaderboard.size(); i++) {
            LeaderboardEntry entry = leaderboard.get(i);
            log(String.format("%2d. %s", i + 1, entry));
        }

        log("-".repeat(70));
    }

    /**
     * Calculates total score across all participants.
     * 
     * @return Sum of all scores
     */
    private Integer calculateTotalScore() {
        return participantScores.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
    }

    // ==================== Step 4: Submit Results ====================

    /**
     * Submits results to the API via POST request.
     * Constructs JSON payload with leaderboard and total score.
     * 
     * @param leaderboard Final leaderboard
     * @param totalScore  Total score across all participants
     */
    private void submitResult(List<LeaderboardEntry> leaderboard, Integer totalScore) {
        log("\n[STEP 4] SUBMITTING RESULTS VIA POST API...");
        log("-".repeat(70));

        try {
            // Build JSON payload
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("regNo", REG_NO);

            List<Map<String, Object>> leaderboardData = new ArrayList<>();
            for (int i = 0; i < leaderboard.size(); i++) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("participant", leaderboard.get(i).participant);
                entry.put("totalScore", leaderboard.get(i).totalScore);
                leaderboardData.add(entry);
            }

            payload.put("leaderboard", leaderboardData);

            String jsonPayload = objectMapper.writeValueAsString(payload);
            log("JSON Request Body:");
            log(objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(payload));

            // Send POST request
            String submitUrl = BASE_URL + "/submit";
            log(String.format("\n→ POST %s", submitUrl));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(submitUrl))
                    .timeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            log(String.format("✓ Response received (status: %d)", response.statusCode()));

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                log("✓ Results submitted successfully!");
                if (response.body() != null && !response.body().isEmpty()) {
                    log("\nPOST Response:");
                    log(objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(objectMapper.readTree(response.body())));
                }
            } else {
                logError(String.format("POST failed with status %d", response.statusCode()));
                if (response.body() != null && !response.body().isEmpty()) {
                    log("Response body: " + response.body());
                }
            }

        } catch (IOException e) {
            logError("Error building JSON payload: " + e.getMessage());
        } catch (InterruptedException e) {
            logError("POST request interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    // ==================== Utility Methods ====================

    /**
     * Logs an informational message with timestamp and formatting.
     */
    private static void log(String message) {
        System.out.println(message);
    }

    /**
     * Logs an error message with ERROR prefix.
     */
    private static void logError(String message) {
        System.out.println("❌ ERROR: " + message);
    }

    // ==================== Entry Point ====================

    /**
     * Main method: Entry point for the application.
     * Instantiates QuizApiIntegration and executes the workflow.
     * 
     * @param args Command-line arguments (not used)
     */
    public static void main(String[] args) {
        QuizApiIntegration integration = new QuizApiIntegration();
        integration.execute();
    }
}
