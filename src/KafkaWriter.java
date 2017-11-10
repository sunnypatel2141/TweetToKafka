import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.*;
import java.util.List;
import java.util.Properties;

public class KafkaWriter
{
	//	private final static String BOOTSTRAP_SERVER = "localhost:9092";
	//	private final static String TOPIC = "first-write";
	//
	//	private static Producer<Long, String> createProducer()
	//	{
	//		Properties props = new Properties();
	//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
	//		                                    BOOTSTRAP_SERVER);
	//		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
	//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	//		                                LongSerializer.class.getName());
	//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	//		                            StringSerializer.class.getName());
	//        return new KafkaProducer<>(props);
	//	}
	//
	//	static void runProducer(final int sendMessageCount) throws InterruptedException
	//	{
	//		final Producer<Long, String> producer = createProducer();
	//		long time = System.currentTimeMillis();
	//		final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);
	//
	//		try
	//		{
	//			for (long index = time; index < time + sendMessageCount; index++)
	//			{
	//				final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, index,
	//						"Using Kafka... ");
	//				producer.send(record, (metadata, exception) ->
	//				{
	//					long elapsedTime = System.currentTimeMillis() - time;
	//					if (metadata != null)
	//					{
	//						System.out.printf("Sent record(key=%s value=%s) " + "Meta(partition=%d, offset=%d) Time=%d\n",
	//								record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
	//					} else
	//					{
	//						exception.printStackTrace();
	//					}
	//					countDownLatch.countDown();
	//				});
	//			}
	//			countDownLatch.await(25, TimeUnit.SECONDS);
	//		} finally
	//		{
	//			producer.flush();
	//			producer.close();
	//		}
	//	}

	
	private List<Tweets> tweets;
	private String topic;
	
	public KafkaWriter()
	{
		// TODO Auto-generated constructor stub
	}

	public boolean tweetToKafka()
	{
		Properties props = assignProperties();
		String schemaString = createSchema();
		parseSchema(schemaString);
		sendRecords(props);
		return true;
	}

	private Properties assignProperties() 
	{
		System.out.println("Assigning properties...");
		
		String url = "http://localhost:8081";
		Properties props = new Properties();
		
		//localhost at default port
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		
		//Confluent Avro Serializer
		props.put("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url", url);
		return props;
	}
	
	private String createSchema()
	{
		String schemaString = 
				"{\"namespace\": \"tweets\", \"type\": \"record\", " +
				"\"name\": \"" + topic + "\"," +
				"\"fields\": [" +
				"{\"name\": \"username\", \"type\": \"string\"}," +
				"{\"name\": \"tweet\", \"type\": \"string\"}" +
				"]}";
		return schemaString;
	}
	
	private void parseSchema(String schemaString)
	{
		System.out.println("Parsing schema...");
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(schemaString);
	}
	
	private void sendRecords(Properties props)
	{
		System.out.println("Sending data to Kafka...");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		for (Tweets tweet : tweets)
		{
			ProducerRecord<String, String> data = new ProducerRecord<String, String>
												(topic, tweet.getUser() + ": ", tweet.getTweet());
			producer.send(data);
		}
		producer.close();
	}

	public static void main(String[] args)
	{
		
	}
	
	public List<Tweets> getTweets()
	{
		return tweets;
	}
	public void setTweets(List<Tweets> tweets)
	{
		this.tweets = tweets;
	}
	public String getTopic()
	{
		return topic;
	}
	public void setTopic(String topic)
	{
		this.topic = topic;
	}
}