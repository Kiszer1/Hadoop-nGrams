* Distributed System Programming: Scale Out with Cloud Computing.
* Hadoop.

* Made by:

Yaad Ben Moshe


* Description :

The program will use the google nGram corpus to calculate the probability for each nGram to appear.
We will be using the deleted estimation method to get the result:

P(<w1, w2, ...., wn>) = (T0_r + T1_r) / ((N0_r + N1_r) * N).

N = Total occurrences count for the entire corpus.
N0_r = Number of nGram types with r occurrences count in group 0. 
N1_r = Number of nGram types with r occurrences count in group 1. 
T0_r = Number of occurrences of nGrams from N0_r in group 1.
T1_r = Number of occurrences of nGrams from N1_r in group 0.



* Running the App:
1. Place aws credentials in ~/.aws/credentials
2. All jar files Count.jar, NTCount.jar, Formula.jar and Sort.jar already uploaded to hadoop bucket.
3. eng-stopwords file uploaded to hadoop bucket.
4. The configuration for the app could be changed in the utilities/Names file
5. Run the local app / main.
6. Wait for app to finish, the program will create an output folder using the randomID as a name, in the hadoop bucket.
7. The output folder will contain the logs and outputs of the program.




* Process:

** Local App / Main:
1. Connects to AWS with credentials
2. Generates a randomID and passes it as an arguement to all the jobs.
3. Configure jobs : count, ntCount, formula and sort.
4. Configures a job flow and run it.


** Count:
1. Assign randomly each nGram instances to group 0 or group 1 and count the occurrences for each nGram in each group.
2. After the job has finished, uploads to s3 the total-count of the occurrences count for the entire corpus: N.


*** Mapper:
1. Gets its input from "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/3gram/data", will work with any google nGram data set.
2. On setup, gets the english stopwords file from s3 and holds it in memory (a small file).
2. For each line from the input, split the line on "/t", run "checkSplit" on the created split to test for :
   1. Split holds an ngram and its occurrences count.
   2. nGram contains no english "stopwords".
   3. nGram contains no chars other then english alphabet and/or spaces
3. If check split returnes false, emit nothing.
4. Else, randomly assign the nGram to group 0 or group 1.
5. Add the occurrences count to a global counter.
6. Emit key-value pair of Ngram as key and a pair holding the occurrences count and group.


*** Combiner:
1. Preforms a local aggregation of the occurrences (addition) for each nGram and its corrosponding group.
2. Emits 2 key-value pairs for each Ngram, 1 for each of the groups with its count.


*** Reducer:
1. Preforms a local aggregation of the occurrences (addition) for each nGram and its corrosponding group.
2. Emits each key with a pair of 2 values, the occurrences count for each group.


Example : 

<dog, cat, fox> counted 300 times for group 0 and 200 times for group 1.
<dog, cat, fox>	300	200 will be emitted.
	
<bee, rat, egg> counted 300 times for group 0 and 50 times for group 1.	
<bee, rat, egg> 300	50 will be emitted.

N = 850
	
** NTCount:
1. Calculates all the N0, N1, T0 and T1 with the corresponding r

*** Mapper:
1. Gets its input from the output created by Count.
2. Each input is an nGram and counts for each group, for each nGram emit 4 key-value pairs:
   1. Key : a pair:
      1. Occurences count for the nGram for each group ("r") 
      2. The group ("N0", "N1", "T0", "T1") 
   2. Value: a pair:
      1. Amout to add to each Group: for N0, N1 we will add 1, for T0 we will add the count for group 1 and for T1 we will add the count for group 0.
      2. the nGram itself.
   		

*** Reducer:
1. Each input will be a unique group of either N0_r / N1_r / T0_r / T1_r
2. The values will be the counts and nGram
3. Simply add all the values- sum, and for each nGram emite a key-value pair: the nGram as key and a pair of the sum and the group (N0 / N1 / T0 / T1).


Example :

Recieved as line input:
<dog, cat, fox>	300	200 
<bee, rat, egg> 300	50

Mapper output:
300	N0	1	<dog, cat, fox>
200	N1	1	<dog, cat, fox>
300	T0	200	<dog, cat, fox>
200	T1	300	<dog, cat, fox>

300	N0	1	<bee, rat, egg>
50	N1	1	<bee, rat, egg>
300	T0	50	<bee, rat, egg> 
50	T1	300	<bee, rat, egg>

reducer output

<dog, cat, fox>	2	N0 (r = 300)
<bee, rat, egg> 2	N0 (r = 300)

<dog, cat, fox>	1	N1 (r = 200)

<bee, rat, egg> 1	N1 (r = 50)

<dog, cat, fox>	250	T0 (r = 300)
<bee, rat, egg> 250	T0 (r = 300)

<dog, cat, fox>	300	T1 (r = 200)

<bee, rat, egg> 300	T1 (r = 50)




** Formula:
1. Get total count from s3, the one uploaded by count and saves it to memory (single double variable).
2. Calculates the formula P(<w1, w2, ...., wn>) = (T0_r + T1_r) / ((N0_r + N1_r) * N) for each nGram.

*** Mapper:
1. Gets its input from the output created by NTCount.
2. Simply emits each line without any changes.


*** Reducer:
1. Each input will be an nGram with its values : N0, N1, T0, T1
2. Will simply calculate the probability for the nGram using the deleted estimation formula.
3. For each nGram, emit a key-value pair: the nGram and its probability.

Example:
With N = 850

Recieved as line input:
<dog, cat, fox>	2	N0	1	N1	250	T0	300	T1

reducer output:
P(<w1, w2, ...., wn>) = (T0_r + T1_r) / ((N0_r + N1_r) * N) ->
P(<dog, cat, fox>) = (250 + 300) / ((1 + 1) * 850) = 550 / 1700 = 0.32352941176

<dog, cat, fox>	0.32352941176 will be emitted.



** Sort:
1. will only sort the input by (1) <w1, w2> ascending , (2) by the probability p(<w1,w2,w3>) descending.

*** Mapper:
1. Gets its input from the output created by Formula.
2. for each line emita key-value pair:
   1. Key : a TextDoubleWritable pair, with the nGram and its probability.
   2. Value: "", will not be used.
3. We will let the partionier sort out the output using the compareTo method defined in our TextDoubleWritable.


*** Reducer:
1. Does nothing but emit the sorted input.


* Hadoop version : 2.10.1

* Instance type used: M4.Large
