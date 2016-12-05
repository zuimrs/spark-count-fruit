hdfs dfs -rm -r /output

spark-submit \
--class samples.SimpleWordCount \
--master yarn \
countFruit.py \
hdfs://Lenovo:9000/input/textFile \
hdfs://Lenovo:9000/output

hdfs dfs -cat /output/*

