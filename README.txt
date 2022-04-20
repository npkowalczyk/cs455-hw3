Team 8
Spencer Howlett
Nick Kowalczyk
Trevor Isaacson

File Manifest:
build.gradle
HW3 Report.pdf
README.txt
cs455-hw3-1.0.jar 
src/main/java/cs455/aqi/q1/Q1Job.java
src/main/java/cs455/aqi/q1/Q1Mapper.java
src/main/java/cs455/aqi/q1/Q1Reducer.java
src/main/java/cs455/aqi/q2/Q2Job.java
src/main/java/cs455/aqi/q2/Q2Mapper.java
src/main/java/cs455/aqi/q2/Q2Reducer.java
src/main/java/cs455/aqi/q3/Q3Job.java
src/main/java/cs455/aqi/q3/Q3Mapper.java
src/main/java/cs455/aqi/q3/Q3Reducer.java
src/main/java/cs455/aqi/q4/Q4Job.java
src/main/java/cs455/aqi/q4/Q4Mapper.java
src/main/java/cs455/aqi/q4/Q4Reducer.java
src/main/java/cs455/aqi/q6/Q6Job.java
src/main/java/cs455/aqi/q6/Q6Mapper.java
src/main/java/cs455/aqi/q6/Q6Reducer.java
Data/parsed.csv


Run Instructions:
1.) gradle build
2.) cp build/libs/cs455-hw3-1.0 .
3.) $HADOOP_HOME/bin/hadoop jar cs455-hw3-1.0.jar cs455.aqi.q{#}.Q{#}Job /fileDirectory /outputDirectory
To run a certain question/folder, simply replace the # with a question/folder number
Ex: $HADOOP_HOME/bin/hadoop jar cs455-hw3-1.0.jar cs455.aqi.q1.Q1Job /fileDirectory /q1output
