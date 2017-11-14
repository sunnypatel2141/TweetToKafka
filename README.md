# Tweet to Kafka

This is an application which searches Twitter for keywords passed in as arguments, or searches for all tweets by a particular user and sends the tweets to Confluent Platform as messages. The username or search value becomes the topic name in Confluent.  

Each tweet is also analyzed using Stanford CoreNLP software for sentiment analysis. The sentiments are broken down into five types: Negative, Slightly negative, Neutral, Slightly positive, and Positive. KafkaWriter produces each record (tweet) to Kafka containing information such as date created, author, sentiment and the tweet itself.  

Make sure you have Confluent running on localhost with Zookeeper node, Kafka Broker, Schema Registry and REST Server. The application assumes bootstrap host is 9092 and schema port is 8081. KafkaWriter.java can be modified to change the server hosts and port numbers.  

Run the TwitterStream.java file with following arguments:  
args[0] = Consumer Key  
args[1] = Consumer Key Secret  
args[2] = Access Token  
args[3] = Access Token Secret  
args[4] = Search keyword  
args[5] = Filename to store record/metadata information  

To consume tweets in Confluent: `./bin/kafka-console-consumer --topic args[4] --bootstrap-server localhost:9092 --from-beginning`  

The jars folder contains some of the jars needed to run the application. It's missing Stanford CoreNLP jars (ejml-xxx.jar, stanford-corenlp-xxx-models.jar and stanford-corenlp-xxx.jar) due to Git's file limit size. These jars needed to downloaded from https://stanfordnlp.github.io/CoreNLP/ and added to build path (Build Path > Configure Build Path > Library > Add External Jars)  

Twitter Streaming API: Twitter4J    
To get API key as well as Access Token: https://apps.twitter.com/    
References: https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens, 
http://twitter4j.org/en/index.html  
Avro Serialization: https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html