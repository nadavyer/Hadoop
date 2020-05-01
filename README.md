#To understand the logic of the probability for the words look at "words probability.jpeg"

Output link on S3: https://s3.console.aws.amazon.com/s3/buckets/hadoopassignment2/output/eedf1939-804b-49d1-b2e0-772b7f12d693-11-final/?region=us-east-1&tab=overview

Jobs Flow:

Step1: create <w1 w2 w3> <number of appearance in corpus = N3> from 3-gram file - N3

Step2: create <w2 w3> <number of appearance in corpus = N2> from 2-gram file - N2

Step3: create <w3> <number of appearance in corpus = N1> from 1-gram file - N1

Step4: Join step1 output and step2 output by common key <w2 w3>. after reduce output is <w1 w2 w3> <N3 N2>

Step5: Join step4 output and step3 output by common key <w3>. after reduce output is <w1 w2 w3> <N3 N2 N1>

Step6: Join step5 output and step2 output by common key <w1 w2>. after reduce output is <w1 w2 w3> <N3 N2 N1 C2>

Step7: Join step6 output and step3 output by common key <w2>. after reduce output is <w1 w2 w3> <N3 N2 N1 C2 C1>

Step8: create <* * *> <total number of words in corpus = C0> by counting output of step3

Step9: Join step7 output and step8 output by common key <* * *>. after reduce output is <w1 w2 w3> <N3 N2 N1 C2 C1 C0>

Step10: calculate the probability with the joined values. after reduce output is <w1 w2 w3> <probability>

Step11: value to key and sort by the first two words (w1 w2) ascending, descending by the probability with compareTo()

 At / run:
		mvn clean compile assembly:single
5. At / run:
		cd target
6. At /target/ run:
		mv dspAss1-1.0-SNAPSHOT-jar-with-dependencies.jar ..
7. At /target/ run:
		cd ..
8. At / run:
		java -jar dspAss1-1.0-SNAPSHOT-jar-with-dependencies.jar
