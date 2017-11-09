package org.twitterapi.kafka.tweet_kafka;

import java.io.IOException;
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
            System.out.println("This application requires five arguments: Consumer Key, Consumer Secret,"
            		+ "Access Token, Access Token Secret, and Search Value.");
            System.exit(-1);
        }
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(args[0])
		  .setOAuthConsumerSecret(args[1])
		  .setOAuthAccessToken(args[2])
		  .setOAuthAccessTokenSecret(args[3]);
		
		System.out.println("Access Token: " + args[2]);
		System.out.println("Access Token Secret: " + args[3]);
		System.out.println("Searching... " + args[4]);
		
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
        try {
            Query query = new Query(args[4]);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
                }
            } while ((query = result.nextQuery()) != null);
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
	}
}
