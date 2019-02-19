git pull; 
sbt package;
/mnt/spark/bin/spark-submit --class com.dp.spark.consumers.realtime.KafkaConsumer \
--jars /mnt/git/dptech/target/scala-2.10/dptech-assembly-0.1.0-deps.jar,/home/hdfs/.ivy2/cache/mysql/mysql-connector-java/jars/mysql-connector-java-5.1.36.jar \
/mnt/git/dptech/target/scala-2.10/dptech_2.10-0.1.0.jar dev

#Elastic search
#1 Write
/usr/hdp/current/spark2-client/bin/spark-submit \
--conf spark.es.nodes=192.168.134.125 \
--conf spark.es.port=9200 \
--conf spark.es.nodes.wan.only=false \
--class com.dp.dptech.es.Write \
--jars /root/spark/spark/target/scala-2.10/dp-spark-assembly-0.1.0-deps.jar,/root/spark/spark/target/scala-2.10/dp-spark_2.10-0.1.0.jar \
/root/spark/spark/target/scala-2.10/dp-spark_2.10-0.1.0.jar
#2 WriteCaseClass
#3 WriteJson
#4 WriteMultipleIndex
#5 WriteWithMeta
#6 WriteWithMetaPlus
#7 WriteWithMetaIDPlus
