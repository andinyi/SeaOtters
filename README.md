# Sea Otters Big Data Project 2
This repository contains all the code and data used for the Sea Otters (Alex Sutherland, Fenix Xia, Handy Ni, and Zach Green) on Project 2 for Revatures Big Data training. The purpose of this project was to query Covid-19 data to answer analytical questions and find trends. This project is a Scala program that can read in data from HDFS, MySQL, or a CSV file. Additionally, command line options allow a user to read batched data and run the program as a multithreaded application.

## What We Wanted to Find :
 - What are the total number of Covid-19 case, total recoveries, and total deaths compared to the worlds population?
 - Which countries have the highest and lowest survival rates of Covid-19?
 - How long does it take for each contry to reach the peak amount of deaths from the first case of Covid-19?
 - What percentage of each countries population has died from Covid-19?  
 - What percentage of each countries population caought Covid-19?
 - What percentage of each countries Covid-19 tests are positive?
 - Is there a correlation between a countries GDP per capita and the countries vaccination rate?
 - What is the connect between a countries GDP per capita, population density, and the rate at which the country reached the peak amount of deaths?
 - How does a countries median age relate to the survival rate?
 - What was a countries average number of new cases per day before and after the vaccine was introduced in that country? 

 ## What's in This Repository :
 - [/src](https://github.com/andinyi/SeaOtters/tree/main/src) contains all the code used in this project.
 - [/resultsCsv](https://github.com/andinyi/SeaOtters/tree/main/resultCsv) contains the results of the analytical questions we asked as CSV files.
- [/datasets](https://github.com/andinyi/SeaOtters/tree/main/datasets) contains all the data we used for this project saved as CSV files. This directory also contains the batched versions of the data we used.