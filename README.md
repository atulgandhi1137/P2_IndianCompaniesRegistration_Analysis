# P2_CompaniesRegistrationData_Analysis
# Project Description
This project aims to analyse Indian Companies Registration data by using Spark and SparkSQL .

# Technologies Used
- Hortonworks Data Platform - 2.6.5
- Apache Spark - version 2.2.0

# Features
## List of features
This project analyzes the companies registration data to get the
- top 50 oldest companies.
- companies which are vanished.
- number of companies registered every year.
- companies whose paidup capital is greater than authorized capital.
- number of companies belonging to specific principal business. etc

# Getting Started
Clone the project
```
$ git clone https://github.com/Mokshesh19/Project2.git
```
# Prerequisite
A Hadoop ecosystem
- Apache Hadoop should be installed with all the components.

# Usage
- Clone the project on your system first because cloning the project directly on your VM locally will not work as the data file is larger in size. 
- If the dataset does not get dowloaded , dowload it from [here](https://www.kaggle.com/rowhitswami/all-indian-companies-registration-data-1900-2019).
- Open command prompt and go to the directory where you have cloned the project and type the command given below
```
$ scp -P 2222 ./registered_companies.csv.zip maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev/folder_name
```
- Now as you have all files on you VM locally,unzip csv file and copy it on to HDFS
```
$ hdfs dfs -put ./registered_companies.csv.zip /user/maria_dev/folder_name
```
- Then, you can run the .py file using sparksubmit.
```
$ spark-submit filename.py
```
