import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KafkaWriter
{
	private List<Tweets> tweets;
	private String topic, filename;
	private boolean fileExistsFlag = false;
	
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
						"{\"name\": \"tweet\", \"type\": [\"null\",\"string\"],\"default\":null}," +
						"{\"name\": \"sentiment\", \"type\": [\"null\",\"string\"],\"default\":null}" +
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
			avroRecord.put("sentiment", tweet.getRating());
			
			ProducerRecord<Object, Object> data = new ProducerRecord<>
													(topic, tweet.getUser(), avroRecord);
			//producer.send(data).get() = synchronous call
			producer.send(data, (metadata, exception) ->
			{
				if (metadata != null)
				{
					if (!fileExistsFlag)
					{
						createFile();
						setFileExistsFlag(true);
					} else {
						populateFile(data, metadata);
					}
				} else
				{
					exception.printStackTrace();
				}
			});
		}
		Logging.print("Finished sending records to Kafka.");
		producer.flush();
		producer.close();
	}

	public static void main(String[] args)
	{
//		KafkaWriter kw = new KafkaWriter();
//		kw.setTopic("sunnypatel2141");
//		kw.setFilename("sunnypate2141.txt");
//		kw.tweetToKafka();
	}
	
	private void populateFile(ProducerRecord<Object, Object> data, RecordMetadata metadata)
	{
		BufferedWriter bw = null;
		FileWriter fw = null;
		try {
			File file = new File(getFilename());
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);

			String str = "Sent record (key=" + data.key() + ", value=" + data.value() + " with " + 
						"Metadata (partition=" + metadata.partition() + ", offset=" + metadata.offset() + ")";
			
			bw.write(str);
			bw.newLine();
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try {
				if (bw != null)
				{
					bw.close();
				}
			
				if (fw != null)
				{
					fw.close();
				}
			} catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public File createFile()
	{
		File file = new File(getFilename());
		try 
		{
			if (!file.exists()) {
				Logging.print("Creating file: " + getFilename());
				file.createNewFile();
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return file;
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
	public void setFilename(String filename)
	{
		this.filename = filename;
	}
	
	public String getFilename()
	{
		return filename;
	}
	public boolean isFileExistsFlag()
	{
		return fileExistsFlag;
	}
	public void setFileExistsFlag(boolean fileExistsFlag)
	{
		this.fileExistsFlag = fileExistsFlag;
	}
}