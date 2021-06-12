# Spotify-Data-Analysis
A Scala implementation

## Introduction 

As a part of one of our courses ; Big Data Analytics, we have completed a project in which we have analysed the various features of a Spotify dataset such as danceability,popularity and valence. We have implemented this using Spark which is an open-source, distributed processing system which is used for big data workloads. It utilizes a technique called 'memory caching' in which computer applications temporarily store data in a computer's main memory (RAM) and optimized query execution for fast queries against data of any size. Simply put, Spark is a fast and general engine for large-scale data processing.

In this project, we have focussed majorly on the following four decades:

80s : 1980-1990

90s : 1990-2000

Early 2ks : 2000-2010

Late 2ks : 2010 to present date


![image](https://user-images.githubusercontent.com/65705774/121775701-d7c5b480-cba6-11eb-8505-72f4a812efdd.png)

We are mainly interested in analysing how people’s taste in music has evolved over these decades.


## Dataset Overview

This dataset contains more than 25 lakh songs collected from Spotify Web API. The features, as mentioned above include song, artist, release date as well as some characteristics of song such as acousticness, danceability, loudness, tempo and so on. The date ranges from 1921 to 2020.

## Analysis
### 1. Number of songs that got released in each decade:

![image](https://user-images.githubusercontent.com/65705774/121775789-4c005800-cba7-11eb-9cac-8ab021e9b8be.png)

#### Conclusion : 
As we can clearly see there is almost two times increase in the number of songs that has been released in the last decade to the previous decades

### 2. Relation Between the Popularity of the Artists and the Popularity of their song:

![image](https://user-images.githubusercontent.com/65705774/121775879-a00b3c80-cba7-11eb-9452-a6fdb5f1a6e1.png)

#### Conclusion : 
It is observed that the popularity of the artists has slightly influenced the popularity of the songs during the first three decades. Surprisingly the percentage of songs from unpopular artists that became popular is very high during this current decade. Therefore we can safely conclude that the current generation’s likes and dislikes depend on the quality of the content and not just mere popularity of the artists.

#### 3. Popularity over the decades:

![image](https://user-images.githubusercontent.com/65705774/121776643-a13e6880-cbab-11eb-912f-539efbb78ef7.png)

#### Conclusion : 
We can clearly see that there is a very clear and gradual increase in the number of songs that became popular as the decades progress. This can also mean that the number of Spotify listeners has grown significantly over the years.


