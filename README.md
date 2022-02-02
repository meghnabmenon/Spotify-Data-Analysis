# Spotify-Data-Analysis
A Scala implementation

## Introduction 

As a part of one of our courses ; Big Data Analytics, we have completed a project in which we have analysed the various features of a Spotify dataset such as danceability,popularity and valence. We have implemented this using Spark which is an open-source, distributed processing system which is used for big data workloads. It utilizes a technique called 'memory caching' in which computer applications temporarily store data in a computer's main memory (RAM) and optimized query execution for fast queries against data of any size. Simply put, Spark is a fast and general engine for large-scale data processing.

In this project, we have focussed majorly on the following four decades:

80s : 1980-1990

90s : 1990-2000

Early 2ks : 2000-2010

Late 2ks : 2010 to present date

We are mainly interested in analysing how people’s taste in music has evolved over these decades.


## Dataset Overview

This dataset contains more than 25 lakh songs collected from Spotify Web API. The features, as mentioned above include song, artist, release date as well as some characteristics of song such as acousticness, danceability, loudness, tempo and so on. The date ranges from 1921 to 2020. The features and what they indicate are briefly explained as and when they are required along this readme file.

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

#### 4. High Valence or Low Valence?

**Valence:** 
  A measure from 0.0 to 1.0 describing the musical positiveness conveyed by a track. Tracks with high valence sound more positive (e.g. happy, cheerful, euphoric), while tracks with low valence sound more negative (e.g. sad, depressed, angry).
  
 ![image](https://user-images.githubusercontent.com/65705774/121776913-0e9ec900-cbad-11eb-848c-4125e395cfbc.png)
 
#### Conclusion : 
 Notice that in most cases in each decade the songs with high valence is slightly more popular but in the case of the current 2ks it is very evident and the difference in the values are noticeable
 
#### 5. Instrumentalism Vs Vocalness

**Instrumentalness :**
Predicts whether a track contains no vocals. The closer the instrumentalness value is to 1.0, the greater likelihood the track contains no vocal content.

![image](https://user-images.githubusercontent.com/65705774/121777095-f24f5c00-cbad-11eb-9d47-59f83d7aa1bc.png)

#### Conclusion : 	
In all the four decades the Spotify listeners have distinctly preferred vocal songs over instrumental music. This information is clearly seen as almost ten times of instrumental song listeners listen to vocal songs.

#### 6. Danceability & Energy:

**Danceability:**  
Danceability describes how suitable a track is for dancing based on a combination of musical elements including tempo, rhythm stability, beat strength, and overall regularity. A value of 0.0 is least danceable and 1.0 is most danceable.
**Energy:** 
Energy is a measure from 0.0 to 1.0 and represents a perceptual measure of intensity and activity. Typically, energetic tracks feel fast, loud, and noisy.

![image](https://user-images.githubusercontent.com/65705774/121777383-5c1c3580-cbaf-11eb-8e8e-2b221359f48b.png)

#### Conclusion : 
In all the four decades the we can safely say that  nearly half or more percentage of danceable songs are high energetic songs. This trend can be especially seen in the early 2ks where almost 71.5 % danceable songs were so high on energy.

#### 7. Speechiness every 2 decades

**Speechiness:**
Speechiness detects the presence of spoken words in a track. The more exclusively speech-like the recording (e.g. talk show, audio book, poetry), the closer to 1.0 the attribute value.

![image](https://user-images.githubusercontent.com/65705774/121777566-7276c100-cbb0-11eb-9ebb-5998aba349c4.png)

#### Conclusion : 
The number of songs with high speechiness is significantly high during the first two decades and it gradually decreases. This implies that podcasts was popular then than now

#### 8. Correlation:
Correlation is a statistic that measures the degree to which two variables move in relation to each other.

**Pearson’s Correlation Coefficient formula:**

![image](https://user-images.githubusercontent.com/65705774/121777831-aa323880-cbb1-11eb-9cc7-71a65e2583f2.png)

##### 8.1 ENERGY AND LOUDNESS

![image](https://user-images.githubusercontent.com/65705774/121777721-2d9f5a00-cbb1-11eb-8029-df094fcd5860.png)

#### Conclusion:
Loudness and Energy share a small positive correlation. 

##### 8.2 ACOUSTICNESS AND LOUDNESS

![image](https://user-images.githubusercontent.com/65705774/121777768-5d4e6200-cbb1-11eb-990a-042a0a721bea.png)

#### Conclusion:
Loudness and Acousticness share a small negative correlation. 

##### 8.3 TEMPO AND ENERGY

![image](https://user-images.githubusercontent.com/65705774/121777796-7fe07b00-cbb1-11eb-819c-6a0d409b7328.png)

#### Conclusion:
Tempo and Energy share a very small positive correlation which can almost be neglected. 

Note: 
The results have been recorded from the outputs obtained after running the scala code. For visualising the analysed values, the plots are created in jupyter notebook.The part of the dataset obtained from kaggle that has been used for the analysis can be found using the following link:

https://amritavishwavidyapeetham-my.sharepoint.com/:f:/g/personal/cb_en_u4aie19030_cb_students_amrita_edu/EiEqzaNHJy9DhwpXgrAEauQBT9mUd67CDI7waO6j7xmPpQ?e=V2umSY



