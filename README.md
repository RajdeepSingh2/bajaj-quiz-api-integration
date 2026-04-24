This project solves a backend integration task involving polling APIs, deduplication, aggregation, and leaderboard generation.
# Quiz API Integration - Bajaj Finserv Health

## Approach
- Called API 10 times (poll=0 to 9) with 5-second delay
- Deduplicated events using (roundId + participant)
- Aggregated scores using HashMap
- Generated leaderboard sorted by totalScore (descending)
- Submitted final leaderboard via POST API

## Tech Stack
- Java 11 HttpClient
- Jackson for JSON parsing

## Output
- Correct leaderboard generated
- Total score computed dynamically
- API submission successful
