git pull; 
sbt package;
/mnt/spark/bin/spark-submit --class com.dp.spark.consumers.realtime.KafkaConsumer \
--jars /mnt/git/dptech/target/scala-2.10/dptech-assembly-0.1.0-deps.jar,/home/hdfs/.ivy2/cache/mysql/mysql-connector-java/jars/mysql-connector-java-5.1.36.jar \
/mnt/git/dptech/target/scala-2.10/dptech_2.10-0.1.0.jar dev
