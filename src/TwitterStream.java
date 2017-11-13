import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Stream twitter
 *
 */
public class TwitterStream 
{	
	private static List<Tweets> allTweets = new ArrayList<>();

	public static void main(String[] args) throws TwitterException, IOException
	{
		if (args.length != 6) {
			System.out.println
        			("This application requires five arguments: Consumer Key, Consumer Secret, "
            		+ "Access Token, Access Token Secret, Search Value, and File Name.");
            System.exit(-1);
        }
		
		String keyword = args[4];
		String filename = args[5];
		
		Logging.print("Keyword: " + keyword);
		
		ConfigurationBuilder cb = assignAccessParams(args);
		Twitter twitter = getInstance(cb);
		
        try {
        		accumulateAllTweets(twitter, keyword);
        } catch (TwitterException te) {
            te.printStackTrace();
            Logging.print("Failed to search tweets: " + te.getMessage());
        }
        
        Logging.print("Finished retrieving tweets...");
        
        instantiateKafkaWriter(keyword, filename);
	}

	private static ConfigurationBuilder assignAccessParams(String[] args)
	{
		Logging.print("Assigning configs...");
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(args[0])
		  .setOAuthConsumerSecret(args[1])
		  .setOAuthAccessToken(args[2])
		  .setOAuthAccessTokenSecret(args[3]);
		return cb;
	}
	
	private static Twitter getInstance(ConfigurationBuilder cb) 
	{
		Logging.print("Create Twitter instance...");
		TwitterFactory tf = new TwitterFactory(cb.build());
		return tf.getInstance();
	}
	
	private static void instantiateKafkaWriter(String topic, String filename)
	{
		Logging.print("Instantiate Kafka Writer...");
		KafkaWriter kafkaWriter = new KafkaWriter();
        kafkaWriter.setTopic(topic);
        kafkaWriter.setTweets(allTweets);
        kafkaWriter.setFilename(filename);
        kafkaWriter.tweetToKafka();
	}
	
	private static void accumulateAllTweets(Twitter twitter, String topic) throws TwitterException
	{
        Query query = new Query(topic);
        QueryResult result;
        do {
            result = twitter.search(query);
            List<Status> tweets = result.getTweets();
            for (Status tweet : tweets) {
            		Tweets tweetObj = new Tweets(tweet.getUser().getScreenName(), 
            				tweet.getText(), tweet.getCreatedAt().toString());
            		allTweets.add(tweetObj);
            }
        } while ((query = result.nextQuery()) != null);
	}
	
	public List<Tweets> getAllTweets()
	{
		return allTweets;
	}
}