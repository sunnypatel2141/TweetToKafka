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
	public static void main(String[] args) throws TwitterException, IOException
	{
		if (args.length != 5) {
            System.out.println
        			("This application requires five arguments: Consumer Key, Consumer Secret,"
            		+ "Access Token, Access Token Secret, and Search Value.");
            System.exit(-1);
        }
		
		List<Tweets> allTweets = new ArrayList<>();
		String keyword = args[4];
		
		System.out.println("Keyword: " + keyword);
		System.out.println("Assigning configs...");
		
		ConfigurationBuilder cb = assignAccessParams(args);
		
		System.out.println("Create Twitter instance...");
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		
        try {
            Query query = new Query(keyword);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                	
                		Tweets tweetObj = new Tweets(tweet.getUser().getScreenName(), tweet.getText());
                		allTweets.add(tweetObj);
                }
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
        }
        
        System.out.println("Calling Kafka Writer...");
        KafkaWriter kafkaWriter = new KafkaWriter();
        kafkaWriter.setTopic(keyword);
        kafkaWriter.setTweets(allTweets);
        kafkaWriter.tweetToKafka();
	}

	private static ConfigurationBuilder assignAccessParams(String[] args)
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(args[0])
		  .setOAuthConsumerSecret(args[1])
		  .setOAuthAccessToken(args[2])
		  .setOAuthAccessTokenSecret(args[3]);
		return cb;
	}
}