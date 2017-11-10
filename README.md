# Tweet to Kafka

This is an application which searches Twitter for keywords passed in as arguments, or searches for all tweets by a particular user and sends the tweets to Confluent Platform as messages. The username or search value becomes the topic name in Confluent.  

Make sure you have Confluent running on localhost with Zookeeper node, Kafka Broker, Schema Registry and REST Server. The application assumes bootstrap host is 9092 and schema port is 8081. KafkaWriter.java can be modified to change the server hosts and port numbers.  

Run the tweet-kafka.jar file with following arguments (`java -jar tweet-kafka.jar`):  
args[0] = Consumer Key  
args[1] = Consumer Key Secret  
args[2] = Access Token  
args[3] = Access Token Secret  
args[4] = Search keyword  

To consume tweets in Confluent: `./bin/kafka-console-consumer --topic args[4] --bootstrap-server localhost:9092 --from-beginning`  

The jars/ folder contains all the necessary jars needed to compile and run the application. Manually add the jars/ library to the build path library (Build Path > Configure Build Path > Library > Add External Jars)  

Twitter Streaming API: Twitter4J    
To get API key as well as Access Token: https://apps.twitter.com/    
References: https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens, 
http://twitter4j.org/en/index.html
