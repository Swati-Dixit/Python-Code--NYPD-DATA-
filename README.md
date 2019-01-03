
Task1: Clean the input data
Have used Spark python code to fetch the required Columb and get the cleaned data into the text file.

Code used is Project2Clean.py (attached in email). Same code is also present on server with the same name. 

Input used on Hadoop cluster : project/NYPD_Motor_Vehicle_WithHeader.txt

OutPut Received : cleaned.txt (On Cluster and also Attached )


Task2:Process the cleaned data for following information 

MapReduce:
 
This step is to implement the MapReduce code for reading the input file and get the requested data for the given details. 
The java code used is Task2.java.
The code includes 2 classes a) Mapper to fetch the values required 

guptasw@hadoop-gate-0:~$ hadoop jar Task2.jar MapReduceTask2.Task2 cleaned.txt FinalOut2

Input Used: cleaned.txt
Output :FinalOut2 (On server) MapReduce-Task2-Out (attached)	

Execution Summary:
guptasw@hadoop-gate-0:~$ hadoop jar Task2.jar MapReduceTask2.Task2 cleaned.txt FinalOut2
18/12/07 21:15:07 INFO client.RMProxy: Connecting to ResourceManager at hdfs-0-3.eecscluster/192.168.200.103:8050
18/12/07 21:15:07 INFO client.AHSProxy: Connecting to Application History server at hdfs-0-0.eecscluster/192.168.200.100:10200
18/12/07 21:15:07 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
18/12/07 21:15:07 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/guptasw/.staging/job_1543342095049_2490
18/12/07 21:15:08 INFO input.FileInputFormat: Total input files to process : 1
18/12/07 21:15:08 INFO mapreduce.JobSubmitter: number of splits:1
18/12/07 21:15:08 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1543342095049_2490
18/12/07 21:15:08 INFO mapreduce.JobSubmitter: Executing with tokens: []
18/12/07 21:15:08 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.0.0.0-1634/0/resource-types.xml
18/12/07 21:15:09 INFO impl.YarnClientImpl: Submitted application application_1543342095049_2490
18/12/07 21:15:09 INFO mapreduce.Job: The url to track the job: http://hdfs-0-3.eecscluster:8088/proxy/application_1543342095049_2490/
18/12/07 21:15:09 INFO mapreduce.Job: Running job: job_1543342095049_2490
18/12/07 21:15:16 INFO mapreduce.Job: Job job_1543342095049_2490 running in uber mode : false
18/12/07 21:15:16 INFO mapreduce.Job:  map 0% reduce 0%
18/12/07 21:15:27 INFO mapreduce.Job:  map 40% reduce 0%
18/12/07 21:15:30 INFO mapreduce.Job:  map 67% reduce 0%
18/12/07 21:15:41 INFO mapreduce.Job:  map 100% reduce 0%
18/12/07 21:15:52 INFO mapreduce.Job:  map 100% reduce 63%
18/12/07 21:15:55 INFO mapreduce.Job:  map 100% reduce 82%
18/12/07 21:15:57 INFO mapreduce.Job:  map 100% reduce 100%
18/12/07 21:15:57 INFO mapreduce.Job: Job job_1543342095049_2490 completed successfully
18/12/07 21:15:57 INFO mapreduce.Job: Counters: 53
	File System Counters
		FILE: Number of bytes read=274336284
		FILE: Number of bytes written=549133003
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=59610656
		HDFS: Number of bytes written=89554
		HDFS: Number of read operations=8
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Rack-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=113300
		Total time spent by all reduces in occupied slots (ms)=141080
		Total time spent by all map tasks (ms)=22660
		Total time spent by all reduce tasks (ms)=14108
		Total vcore-milliseconds taken by all map tasks=22660
		Total vcore-milliseconds taken by all reduce tasks=14108
		Total megabyte-milliseconds taken by all map tasks=116019200
		Total megabyte-milliseconds taken by all reduce tasks=144465920
	Map-Reduce Framework
		Map input records=967234
		Map output records=7737872
		Map output bytes=258860534
		Map output materialized bytes=274336284
		Input split bytes=122
		Combine input records=0
		Combine output records=0
		Reduce input groups=2956
		Reduce shuffle bytes=274336284
		Reduce input records=7737872
		Reduce output records=2956
		Spilled Records=15475744
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=942
		CPU time spent (ms)=44000
		Physical memory (bytes) snapshot=3515977728
		Virtual memory (bytes) snapshot=16934051840
		Total committed heap usage (bytes)=3790077952
		Peak Map Physical memory (bytes)=2844971008
		Peak Map Virtual memory (bytes)=6228291584
		Peak Reduce Physical memory (bytes)=671006720
		Peak Reduce Virtual memory (bytes)=10705760256
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=59610534
	File Output Format Counters 
		Bytes Written=89554


Step3: Process output file from Task#2 and generate final output file with all the required numbers from Task#2 
This step is to consolidate all the data and give the final output. Below is the command used and output received. 
Code used is Task3.Java

guptasw@hadoop-gate-0:~$ hadoop jar Task3.jar MapReduceTask3.Task3
1. Date on which maximum number of accidents took place:  Accident_Date-11/26/2013  394
2. Borough with maximum count of accident fatality:  Borough-BROOKLYN 663
3. Zip with maximum count of accident fatality:  ZipCode-11236 54
4. Which vehicle type is involved in maximum accidents:  Vehicle-Type1-PASSENGER VEHICLE 253470
5. Year in which maximum Number Of Persons and Pedestrians Injured:  Persons-Pedestrains-Injured-2013 52791
6. Year in which maximum Number Of Persons and Pedestrians Killed:  Persons-Pedestrains-Killed-2013 345
7. Year in which maximum Number Of Cyclist Injured and Killed:  Cyclist-Injured-Killed-2015 3876
8. Year in which maximum Number Of Motorist Injured and Killed:  Motorists-Killed-Injured-2013 27622

Input used: ResultOut-Task2/part-r-00000
OutPut Created : MapReduce-Task3-Out (attached)



