public class Tweets
{
	private String createdAt, user, tweet;

	public Tweets(String user, String tweet, String createdAt)
	{
		setUser(user);
		setTweet(tweet);
		setCreatedAt(createdAt);
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
	public String getCreatedAt()
	{
		return createdAt;
	}
	public void setCreatedAt(String createdAt)
	{
		this.createdAt = createdAt;
	}
}