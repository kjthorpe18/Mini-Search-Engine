
# Remove an old jar from the hadoop filesystem
hadoop fs -rm WordCount.jar

# Compile project and create a jar
hadoop com.sun.tools.javac.Main WordCount.java
jar cf WordCount.jar *.class

# Copy jar over to hadoop fs and GCP Datastore
hadoop fs -copyFromLocal ./WordCount.jar
hadoop fs -cp ./WordCount.jar gs://dataproc-staging-us-central1-1093852109547-d5pnwsmu/JAR

# Execute the job
hadoop jar WordCount.jar WordCount gs://dataproc-staging-us-central1-1093852109547-d5pnwsmu/test-data gs://dataproc-staging-us-central1-1$

# Merge output files and create a single output.txt file, put it in GCP Datastore
hadoop fs -getmerge gs://dataproc-staging-us-central1-1093852109547-d5pnwsmu/output ./output.txt
hadoop fs -copyFromLocal ./output.txt
hadoop fs -cp ./output.txt gs://dataproc-staging-us-central1-1093852109547-d5pnwsmu/output.txt