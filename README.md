# Tweet to Kafka

This is an application which searches Twitter for keywords passed in as arguments, or searches for all tweets by a particular user and sends the tweets to Confluent Platform as messages. The username or search value becomes the topic name in Confluent.  

Run the Java Application with following arguments:  
args[0] = Consumer Key  
args[1] = Consumer Key Secret  
args[2] = Access Token  
args[3] = Access Token Secret  
args[4] = Search keyword  

Twitter Streaming API: Twitter4J    
To get API key as well as Access Token: https://apps.twitter.com/    
References: https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens
