# WeblogChallenge
This is the solution program to the interview challenge for Paytm Labs written in spark-scala. 

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Results

1. The web log is sessionized by IP and time window using three inactivity thresholds: 15 min (iat_1), 30 min (iat_2), and 45 min (iat_3)

2. The average session time is 1.68 min (iat_1), 2.74 min (iat_2), and 2.83 min (iat_3)

3) The average unique urls visited per session is 8.30 (iat_1), 8.61 (iat_2), and 8.63 (iat_3)

4) The most engaged user is 119.81.61.166 (34.48 min) (iat_1), 220.226.206.7 (54.15 min) (iat_2), 220.226.206.7 (54.15 min) (iat_3)
