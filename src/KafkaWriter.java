import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
		Schema schema = parseSchema(schemaString);
		sendRecords(props, schema);
		return true;
	}

	private Properties assignProperties() 
	{
		Logging.print("Assigning properties...");

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
						"{\"name\": \"createdAt\", \"type\": [\"null\",\"string\"],\"default\":null}," +
						"{\"name\": \"username\", \"type\": [\"null\",\"string\"],\"default\":null}," +
						"{\"name\": \"tweet\", \"type\": [\"null\",\"string\"],\"default\":null}" +
						"]}";
		return schemaString;
	}

	private Schema parseSchema(String schemaString)
	{
		Logging.print("Parsing schema...");
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(schemaString);
		return schema;
	}

	private void sendRecords(Properties props, Schema schema)
	{
		Logging.print("Sending records to Kafka...");

		KafkaProducer<Object, Object> producer = new KafkaProducer<Object, Object>(props);

		for (Tweets tweet : tweets)
		{
			GenericRecord avroRecord = new GenericData.Record(schema);
			avroRecord.put("createdAt", tweet.getCreatedAt());
			avroRecord.put("username", tweet.getUser());
			avroRecord.put("tweet", tweet.getTweet());

			System.out.println("Sending tweet ...");
			ProducerRecord<Object, Object> data = new ProducerRecord<>
													(topic, tweet.getUser(), avroRecord);
			//producer.send(data).get() = synchronous call
			producer.send(data);
		}
		Logging.print("Finished sending records to Kafka...");
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