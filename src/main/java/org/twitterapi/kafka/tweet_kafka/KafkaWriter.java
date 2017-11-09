package org.twitterapi.kafka.tweet_kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaWriter
{
	private final static String BOOTSTRAP_SERVER = "localhost:9092";
	private final static String TOPIC = "first-write";

	private static Producer<Long, String> createProducer()
	{
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<Long, String>(props);
	}

	static void runProducer(final int sendMessageCount) throws InterruptedException
	{
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();
		final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

		try
		{
			for (long index = time; index < time + sendMessageCount; index++)
			{
				final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, index,
						"Using Kafka... ");
				producer.send(record, (metadata, exception) ->
				{
					long elapsedTime = System.currentTimeMillis() - time;
					if (metadata != null)
					{
						System.out.printf("Sent record(key=%s value=%s) " + "Meta(partition=%d, offset=%d) Time=%d\n",
								record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
					} else
					{
						exception.printStackTrace();
					}
					countDownLatch.countDown();
				});
			}
			countDownLatch.await(25, TimeUnit.SECONDS);
		} finally
		{
			producer.flush();
			producer.close();
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length == 0) {
	        runProducer(5);
	    } else {
	        runProducer(Integer.parseInt(args[0]));
	    }
	}
}
