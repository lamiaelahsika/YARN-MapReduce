#TP YARN MapReduce2

# 1.YARN

##1.2 Run the wordcount example

##### 1. Connect to HADOOP cluster using SSH

	> ssh llahsika@hadoop-edge01.efrei.online

#####2.From the SSH session, use the following command to list the samples:

			-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar
			
	An example program must be given as the first argument.
	Valid program names are:
	  aggregatewordcount: An Aggregate based map/reduce program that counts the words in the input files.
	  aggregatewordhist: An Aggregate based map/reduce program that computes the histogram of the words in the input files.
	  bbp: A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact digits of Pi.
	  dbcount: An example job that count the pageview counts from a database.
	  distbbp: A map/reduce program that uses a BBP-type formula to compute exact bits of Pi.
	  grep: A map/reduce program that counts the matches of a regex in the input.
	  join: A job that effects a join over sorted, equally partitioned datasets
	  multifilewc: A job that counts words from several files.
	  pentomino: A map/reduce tile laying program to find solutions to pentomino problems.
	  pi: A map/reduce program that estimates Pi using a quasi-Monte Carlo method.
	  randomtextwriter: A map/reduce program that writes 10GB of random textual data per node.
	  randomwriter: A map/reduce program that writes 10GB of random data per node.
	  secondarysort: An example defining a secondary sort to the reduce.
	  sort: A map/reduce program that sorts the data written by the random writer.
	  sudoku: A sudoku solver.
	  teragen: Generate data for the terasort
	  terasort: Run the terasort
	  teravalidate: Checking results of terasort
	  wordcount: A map/reduce program that counts the words in the input files.
	  wordmean: A map/reduce program that counts the average length of the words in the input files.
	  wordmedian: A map/reduce program that counts the median length of the words in the input files.
	  wordstandarddeviation: A map/reduce program that counts the standard deviation of the length of the words in the input files.	

#####3.get help on a specific sample
	-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \wordcount

	 Usage: wordcount <in> [<in>...] <out>
##### 4. count all words in downloaded e-book
	-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \wordcount /user/llahsika/davinci.txt /user/llahsika/wordcount
	
##### 5
	  -sh-4.2$ hdfs dfs -cat /user/llahsika/wordcount/ part-r-00000


##1.3 The Sudoku example
		-sh-4.2$  cat  sudoku.dta
	> 8 5 ? 3 9 ? ? ? ?
	? ? 2 ? ? ? ? ? ?
	? ? 6 ? 1 ? ? ? 2
	? ? 4 ? ? 3 ? 5 9
	? ? 8 9 ? 1 4 ? ?
	3 2 ? 4 ? ? 8 ? ?
	9 ? ? ? 8 ? 5 ? ?
	? ? ? ? ? ? 2 ? ?
	? ? ? ? 4 5 ? 7 8

	-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \sudoku sudoku.dta
	
	Solving sudoku.dta
	8 5 1 3 9 2 6 4 7 
	4 3 2 6 7 8 1 9 5 
	7 9 6 5 1 4 3 8 2 
	6 1 4 8 2 3 7 5 9 
	5 7 8 9 6 1 4 2 3 
	3 2 9 4 5 7 8 1 6 
	9 4 7 2 8 6 5 3 1 
	1 8 5 7 3 9 2 6 4 
	2 6 3 1 4 5 9 7 8 
	
	Found 1 solutions

##1.4 Pi example


	-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \pi 16 10000000
	
	Number of Maps  = 16
	Samples per Map = 10000000
	Wrote input for Map #0
	Wrote input for Map #1
	Wrote input for Map #2
	Wrote input for Map #3
	Wrote input for Map #4
	Wrote input for Map #5
	Wrote input for Map #6
	Wrote input for Map #7
	Wrote input for Map #8
	Wrote input for Map #9
	Wrote input for Map #10
	Wrote input for Map #11
	Wrote input for Map #12
	Wrote input for Map #13
	Wrote input for Map #14
	Wrote input for Map #15
	Starting Job
	20/10/19 01:56:10 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
	20/10/19 01:56:10 INFO hdfs.DFSClient: Created token for llahsika: HDFS_DELEGATION_TOKEN owner=llahsika@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1603065370147, maxDate=1603670170147, sequenceNumber=1238, masterKeyId=23 on ha-hdfs:efrei
	20/10/19 01:56:10 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for llahsika: HDFS_DELEGATION_TOKEN owner=llahsika@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1603065370147, maxDate=1603670170147, sequenceNumber=1238, masterKeyId=23)
	20/10/19 01:56:10 INFO client.ConfiguredRMFailoverProxyProvider: Failing over to rm2
	20/10/19 01:56:10 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/llahsika/.staging/job_1602414795136_0803
	20/10/19 01:56:10 INFO input.FileInputFormat: Total input files to process : 16
	20/10/19 01:56:10 INFO mapreduce.JobSubmitter: number of splits:16
	20/10/19 01:56:10 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1602414795136_0803
	20/10/19 01:56:10 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for llahsika: HDFS_DELEGATION_TOKEN owner=llahsika@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1603065370147, maxDate=1603670170147, sequenceNumber=1238, masterKeyId=23)]
	20/10/19 01:56:10 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
	20/10/19 01:56:10 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
	20/10/19 01:56:11 INFO impl.YarnClientImpl: Submitted application application_1602414795136_0803
	20/10/19 01:56:11 INFO mapreduce.Job: The url to track the job: https://hadoop-master02.efrei.online:8090/proxy/application_1602414795136_0803/
	20/10/19 01:56:11 INFO mapreduce.Job: Running job: job_1602414795136_0803
	20/10/19 01:56:19 INFO mapreduce.Job: Job job_1602414795136_0803 running in uber mode : false
	20/10/19 01:56:19 INFO mapreduce.Job:  map 0% reduce 0%
	20/10/19 01:56:30 INFO mapreduce.Job:  map 100% reduce 0%
	20/10/19 01:56:37 INFO mapreduce.Job:  map 100% reduce 100%
	20/10/19 01:56:38 INFO mapreduce.Job: Job job_1602414795136_0803 completed successfully
	20/10/19 01:56:38 INFO mapreduce.Job: Counters: 53
		File System Counters
			FILE: Number of bytes read=358
			FILE: Number of bytes written=4199672
			FILE: Number of read operations=0
			FILE: Number of large read operations=0
			FILE: Number of write operations=0
			HDFS: Number of bytes read=4134
			HDFS: Number of bytes written=215
			HDFS: Number of read operations=69
			HDFS: Number of large read operations=0
			HDFS: Number of write operations=3
		Job Counters 
			Launched map tasks=16
			Launched reduce tasks=1
			Data-local map tasks=16
			Total time spent by all maps in occupied slots (ms)=351312
			Total time spent by all reduces in occupied slots (ms)=20340
			Total time spent by all map tasks (ms)=117104
			Total time spent by all reduce tasks (ms)=5085
			Total vcore-milliseconds taken by all map tasks=117104
			Total vcore-milliseconds taken by all reduce tasks=5085
			Total megabyte-milliseconds taken by all map tasks=179871744
			Total megabyte-milliseconds taken by all reduce tasks=10414080
		Map-Reduce Framework
			Map input records=16
			Map output records=32
			Map output bytes=288
			Map output materialized bytes=448
			Input split bytes=2246
			Combine input records=0
			Combine output records=0
			Reduce input groups=2
			Reduce shuffle bytes=448
			Reduce input records=32
			Reduce output records=0
			Spilled Records=64
			Shuffled Maps =16
			Failed Shuffles=0
			Merged Map outputs=16
			GC time elapsed (ms)=2682
			CPU time spent (ms)=39740
			Physical memory (bytes) snapshot=18784841728
			Virtual memory (bytes) snapshot=58137350144
			Total committed heap usage (bytes)=19474153472
			Peak Map Physical memory (bytes)=1166487552
			Peak Map Virtual memory (bytes)=3394334720
			Peak Reduce Physical memory (bytes)=285405184
			Peak Reduce Virtual memory (bytes)=3874410496
		Shuffle Errors
			BAD_ID=0
			CONNECTION=0
			IO_ERROR=0
			WRONG_LENGTH=0
			WRONG_MAP=0
			WRONG_REDUCE=0
		File Input Format Counters 
			Bytes Read=1888
		File Output Format Counters 
			Bytes Written=97
	Job Finished in 28.483 seconds
	> Estimated value of Pi is 3.14159155000000000000

##1.5 10 GB GraySort example

	-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \teragen -Dmapred.map.tasks=50 100000000/user/llahsika/data/10GB-sort-input
	teragen <num rows> <output dir>
	If you want to generate data and store them as erasure code striping file, just make sure that the parent dir of <output dir> has erasure code policy 
	
	-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \terasort -Dmapred.map.tasks=50 -Dmapred.reduce.tasks=25/user/llahsika/data/10GB-sort-
			Usage: terasort [-Dproperty=value] <in> <out>
			TeraSort configurations are:
			<mapreduce.terasort.num-rows>     Number of rows to generate during teragen.
			<mapreduce.terasort.num.partitions>     Number of partitions used for sampling.
			<mapreduce.terasort.partitions.sample>     Sample size for each partition.
			<mapreduce.terasort.final.sync>     Perform a disk-persisting hsync at end of every file-write.
			<mapreduce.terasort.use.terascheduler>     Use TeraScheduler for computing input split distribution.
			<mapreduce.terasort.simplepartitioner>     Use SimplePartitioner instead of TotalOrderPartitioner.
			<mapreduce.terasort.output.replication>     Replication factor to use for output data files.
			If you want to store the output data as erasure code striping file, just make sure that the parent dir of <out> has erasure code policy set
	
	-sh-4.2$ yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \teravalidate  -Dmapred.map.tasks=50 -Dmapred.reduce.tasks=25/user/llahsika/data/10GB-
	teravalidate <out-dir> <report-dir> 

###2.3.1 Map step: mapper.py
	#! /usr/bin/env python
	""" mapper .py """
	import sys
	# input comes from STDIN ( standard input )
	for line in sys . stdin :
	# remove leading and trailing whitespace
	    line = line.strip()
	# split the line into words
	    words=line.split(", ")
	# increase counters
	for word in words :
	# write the results to STDOUT ( standard output );
	# what we output here will be the input for the
	# Reduce step , i.e. the input for reducer .py
	
	# tab - delimited ; the trivial word count is 1
	    print  '%s\t%s' % (word, 1)
	  
### 2.3.2 Reduce step: reducer.py

	#!/usr/bin/env python
	"""reducer.py"""
	
	from operator import itemgetter
	import sys
	
	current_word = None
	current_count = 0
	word = None
	
	# input comes from STDIN
	for line in sys.stdin:
	# remove leading and trailing whitespace
	    line = line.strip()
	
	# parse the input we got from mapper.py
	    word, count = line.split('\t', 1)
	
	# convert count (currently a string) to int
	    try:
	        count = int(count)
	    except ValueError:
	        continue
	# this IF-switch only works because Hadoop sorts map output
	# by key (here: word) before it is passed to the reducer
	    if current_word == word:
	        current_count += count
	    else:
	        if current_word:
	            print '%s\t%s' % (current_word, current_count)
	        current_count = count
	        current_word = word
	
	# do not forget to output the last word if needed!
	if current_word == word:
	    print '%s\t%s' % (current_word, current_count)
	    
###2.3.3 Test your code (cat data | map | sort | reduce)
	 -sh-4.2$ echo "foo foo quux labs foo bar quux" | /home/llahsika/mapper.py
	foo     1
	foo     1
	quux    1
	labs    1
	foo     1
	bar     1
	quux    1
	

	 -sh-4.2$ echo "foo foo quux labs foo bar quux" | /home/llahsika/mapper.py | sort -k1,1 | /home/llahsika/reducer.py
	bar     1
	foo     3
	labs    1
	quux    2
	
##2.4 Running the Python Code on Hadoop

###2.4.1 Download example input data

	-sh-4.2$ wget http://www.gutenberg.org/ebooks/4300
	--2020-10-19 23:38:39--  http://www.gutenberg.org/ebooks/4300
	Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
	Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:80... connected.
	HTTP request sent, awaiting response... 200 OK
	Length: 21226 (21K) [text/html]
	Saving to: '4300'
	
	100%[==================================================================================================================================================================>] 21,226      --.-K/s   in 0.09s   
	
	2020-10-19 23:38:39 (221 KB/s) - '4300' saved [21226/21226]

	-sh-4.2$ wget http://www.gutenberg.org/ebooks/20417
	--2020-10-19 23:37:18--  http://www.gutenberg.org/ebooks/20417
	Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
	Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:80... connected.
	HTTP request sent, awaiting response... 200 OK
	Length: 18802 (18K) [text/html]
	Saving to: '20417'
	
	100%[==================================================================================================================================================================>] 18,802      --.-K/s   in 0.09s   
	
	2020-10-19 23:37:19 (198 KB/s) - '20417' saved [18802/18802]
	
			
	-sh-4.2$ wget http://www.gutenberg.org/ebooks/5000
	--2020-10-19 23:42:04--  http://www.gutenberg.org/ebooks/5000
	Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
	Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:80... connected.
	HTTP request sent, awaiting response... 200 OK
	Length: 19659 (19K) [text/html]
	Saving to: '5000'
	
	100%[==================================================================================================================================================================>] 19,659      --.-K/s   in 0.09s   
	
	2020-10-19 23:42:04 (207 KB/s) - '5000' saved [19659/19659]
	
	 -sh-4.2$ ls -l /tmp/gutenberg/
	 
##2.4.2 Copy local example data to HDFS

	-sh-4.2$ /usr/local/hadoop$ bin/hadoop dfs -copyFromLocal /tmp/gutenberg /user/llahsika/gutenberg
	-sh-4.2$ /usr/local/hadoop$ bin/hadoop dfs -ls
			Found 1 items
			drwxr-xr-x   - llahsika supergroup       0 2010-05-08 17:40 /user/llahsika/gutenberg
			
##2.4.3 Run the MapReduce job
	-sh-4.2$ /usr/local/hadoop$ bin/hadoop jar contrib/streaming/hadoop-*streaming*.jar \
	-file /home/llahsika/mapper.py    -mapper /home/llahsika/mapper.py \
	-file /home/llahsika/reducer.py   -reducer /home/llahsika/reducer.py \
	-input /user/llahsika/gutenberg/* -output /user/llahsika/gutenberg-output
	
	-sh-4.2$ /usr/local/hadoop$ bin/hadoop jar contrib/streaming/hadoop-*streaming*.jar -D mapred.reduce.tasks=16 ...
	
##2.4.5 mapper.py
	#!/usr/bin/env python
	"""A more advanced Mapper, using Python iterators and generators."""
	
	import sys
	
	def read_input(file):
	    for line in file:
	        # split the line into words
	        yield line.split()
	
	def main(separator='\t'):
	    # input comes from STDIN (standard input)
	    data = read_input(sys.stdin)
	    for words in data:
	        # write the results to STDOUT (standard output);
	        # what we output here will be the input for the
	        # Reduce step, i.e. the input for reducer.py
	        #
	        # tab-delimited; the trivial word count is 1
	        for word in words:
	            print '%s%s%d' % (word, separator, 1)
	
	if __name__ == "__main__":
	    main()
##2.4.6 reducer.py
		#!/usr/bin/env python
	"""A more advanced Reducer, using Python iterators and generators."""
	
	from itertools import groupby
	from operator import itemgetter
	import sys
	
	def read_mapper_output(file, separator='\t'):
	    for line in file:
	        yield line.rstrip().split(separator, 1)
	
	def main(separator='\t'):
	    # input comes from STDIN (standard input)
	    data = read_mapper_output(sys.stdin, separator=separator)
	    # groupby groups multiple word-count pairs by word,
	    # and creates an iterator that returns consecutive keys and their group:
	    #   current_word - string containing a word (the key)
	    #   group - iterator yielding all [”< current_word>”,”<count >”]  items
	    for current_word, group in groupby(data, itemgetter(0)):
	        try:
	            total_count = sum(int(count) for current_word, count in group)
	            print "%s%s%d" % (current_word, separator, total_count)
	        except ValueError:
	            
	
	if __name__ == "__main__":
	    main()
	

			