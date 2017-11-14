import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/** * Stream twitter * */
public class TwitterStream
{
	 static class SimpleThread extends Thread 
	 { 
		 Tweets tweets;
		 
		 public SimpleThread(Tweets tweets) 
		 { 
//			 System.out.println("Initialize thread ... " + Thread.currentThread().getName());
			 this.tweets = tweets;
		 }
		 public void run() 
		 { 
//			 System.out.println("Starting thread ... " + Thread.currentThread().getName());
			 int score = NLP.findSentiment(tweets.getTweet());
			 tweets.setRating(stringify(score));
		 }
	 }

	private static Vector<Tweets> allTweets = new Vector<>();

	public static void main(String[] args) throws TwitterException, IOException
	{
		if (args.length != 6)
		{
			System.out.println("This application requires five arguments: Consumer Key, Consumer Secret, "
					+ "Access Token, Access Token Secret, Search Value, and File Name.");

			System.exit(-1);

		}
		String keyword = args[4];
		String filename = args[5];
		Logging.print("Keyword: " + keyword);
		TwitterStream ts = new TwitterStream();
		ConfigurationBuilder cb = ts.assignAccessParams(args);
		Twitter twitter = ts.getInstance(cb);
		try
		{
			ts.accumulateAllTweets(twitter, keyword);
		} catch (TwitterException te)
		{
			te.printStackTrace();
			Logging.print("Failed to search tweets: " + te.getMessage());
		}
		Logging.print("Finished retrieving tweets...");
		// ts.printContents();
		
		ts.analyzeTweets();
		ts.instantiateKafkaWriter(keyword, filename);
	}

	private ConfigurationBuilder assignAccessParams(String[] args)
	{
		Logging.print("Assigning configs...");
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(args[0]).setOAuthConsumerSecret(args[1])
				.setOAuthAccessToken(args[2]).setOAuthAccessTokenSecret(args[3]);
		return cb;
	}

	private Twitter getInstance(ConfigurationBuilder cb)
	{
		Logging.print("Create Twitter instance...");
		TwitterFactory tf = new TwitterFactory(cb.build());
		return tf.getInstance();
	}

	private void instantiateKafkaWriter(String topic, String filename)
	{
		Logging.print("Instantiate Kafka Writer...");
		KafkaWriter kafkaWriter = new KafkaWriter();
		kafkaWriter.setTopic(topic);
		kafkaWriter.setTweets(allTweets);
		kafkaWriter.setFilename(filename);
		kafkaWriter.tweetToKafka();
	}

	private void accumulateAllTweets(Twitter twitter, String topic) throws TwitterException
	{
		Query query = new Query(topic);
		QueryResult result;
		// ArrayList<SimpleThread> arrThreads = new ArrayList<SimpleThread>();
		
		long start = System.currentTimeMillis();
		do
		{
			result = twitter.search(query);
			List<Status> tweets = result.getTweets();
			for (Status tweet : tweets)
			{
				Tweets tweetObj = new Tweets(tweet.getUser().getScreenName(), tweet.getText(),
						tweet.getCreatedAt().toString());
				allTweets.add(tweetObj);
			}
		} while ((query = result.nextQuery()) != null);
		
//		 do { result = twitter.search(query);
//			SimpleThread st = new SimpleThread(result);
//			st.start();
//			arrThreads.add(st);
//		 	if (arrThreads.size() % 10 == 0) 
//			{ 
//			try 
//				{ 
//					//sleep for five seconds every 10 thread
//					Thread.sleep(5000);
//				} catch (InterruptedException e) 
//				{ 
//					// // TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		} while ((query = result.nextQuery()) != null);
		
		long end = System.currentTimeMillis();
		System.out.println("Total time: " + (end - start));
		
//		 try 
//		 { 
//			 for (int i = 0; i < arrThreads.size(); i++) 
//			 {
//				 arrThreads.get(i).join();
//			 } 
//		 } catch (InterruptedException e) 
//		 { 
//			 e.printStackTrace();
//		 }
	}
		 
//	public void printContents() 
//	{ 
//		for (Tweets tweet : allTweets)
//		{
//			System.out.println(tweet.getCreatedAt() + "..." + tweet.getTweet() + "..." + tweet.getUser());
//		 }
//	}

	public void analyzeTweets()
	{
		long time = System.currentTimeMillis();
		ArrayList<SimpleThread> arrThreads = new ArrayList<SimpleThread>();
		NLP.init();	
//		ExecutorService executor = Executors.newFixedThreadPool(100);
		for (Tweets tweets : allTweets)
		{
//			System.out.println("Inside...");
//			Runnable worker = new SimpleThread(tweets);
//			executor.execute(worker);
			SimpleThread st = new SimpleThread(tweets);
			st.run();
			arrThreads.add(st);
		}
//		try
//		{
//			executor.awaitTermination(10, TimeUnit.SECONDS);
//		} catch (InterruptedException e)
//		{
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		executor.shutdown();
//        while (!executor.isTerminated()) { }

        long time2 = System.currentTimeMillis();
		System.out.println("Analyze time: " + (time2-time));
		
		try
		{
			for (int i = 0 ; i < arrThreads.size(); i++)
			{
				arrThreads.get(i).join();
			}
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
	
	private static String stringify(int score)
	{
		String str;
		switch (score)
		{
			case 0:
				str = "SENTIMENT: Negative";
				break;
			case 1:
				str = "SENTIMENT: Slightly negative";
				break;
			case 2:
				str = "SENTIMENT: Neutral";
				break;
			case 3:
				str = "SENTIMENT: Slightly positive";
				break;
			default:
				str = "SENTIMENT: Positive";
				break;
		}
		return str;
	}

	public Vector<Tweets> getAllTweets()
	{
		return allTweets;
	}
}