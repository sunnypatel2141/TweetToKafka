public class Tweets
{
	private String user, tweet;

	public Tweets(String user, String tweet)
	{
		setUser(user);
		setTweet(tweet);
	}

	public String getUser()
	{
		return user;
	}
	public void setUser(String user)
	{
		this.user = user;
	}
	public String getTweet()
	{
		return tweet;
	}
	public void setTweet(String tweet)
	{
		this.tweet = tweet;
	}	
}