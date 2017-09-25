# CSMDEMO

This is a demo project, demonstrating a possible implementation of a leaderboard for an on-line gaming company.

## Calculation

The objective is to calculate a GRV score. There are two sources of data, game data and player data. 

The GRV is calculated as follows:

```
if      PGL(p,g)  >   0 :
    GRV (g,   p)    =   PGL (p,g)    *   SUM(PIS (i,   p)    *   PGL (i,g))
else:
    GRV (g,   p)    =   SUM(PIS (i,   p)    *   PGL (i,g)) 
```

where p the player, g the game and i the influencing player.

### Player data (PIS)
From the player data we will be calculating the PIS score based on the following mapping for event types:

| Event type | Influence points | Player ID influenced (p) | Player ID influencing (i) |
| AchievementLikedEvent | +1 | achievement_owner_id | follower_id |
| AchievementCommentedEvent | +2 | achievement_owner_id | follower_id |
| AchievementSharedEvent | +2 | follower_id | achievement_owner_id |
| ProfileVisitedEvent | +5 | visitor_profile_id | user_id |
| DirectMessageSentEvent | +3 | target_profile_id | source_profile_id |

Recent occurrences will have a 1.5 weight when their score is calculated where recent is assumed to be 1 day from the execution time.

### Game data (PGL)

From the game data we will be calculating the PGL score using the following mapping for event types:
| Event type| Score | Player ID (i or p) | Game (g) |
| GameRoundStarted | +1 | user_id | game_id |
| GameRoundEnded | +1 | user_id | game_id |
| GameRoundDropped | -20 | user_id | game_id |
| GameWinEvent | +3 | user_id | game_id |
| MajorWinEvent | +7 | user_id | game_id |


## Approach

There are probably many approaches one can take to calculate the required score, but I have considered the two most obvious to me: 

1. For each game (g):
    a. calculate PIS(i, p) as the total score per i.
    b. calculate PGL(i, g) as the sum per i of the game data.
    c. join PIS(i, p) with PGL(i, g) on i.
    d. create new column PIS(i, p) * PGL(i, g).
    e. group on p and SUM() the new column from d.
    f. join the result with PGL(p, g) on p (p and i are the same on PGL)
    g. multiply with max(PGL(p), 1) to obtain GRV(p,g) for that game.

2. For each game (g) start similarly with:
    a. calculate PIS(i, p) as the total score per i.
    b. calculate PGL(i, g) as the sum per i of the game data.
    c. Create a graph using the two and use that to perform the same calculations

The second solution may be a good idea if more complex processing is required, but for the purpose of this simple demo it is a distraction. 

## Assumptions

1. There is one input file of each player and game data.
2. The data is well formed.
3. The data always contains the fields required and they are of the same type. 

## Possible future steps

1. Create schemata for the data in the from of avro (or similar) and use those for code generation.
2. A schema will also speed up loading the data.

## Deploy

In order to deploy just run

```
sbt assembly
```
which will first run the tests and then use the resulting assembly jar (depending on platform other dependencies may be missing, e.g. aws-hadoop)