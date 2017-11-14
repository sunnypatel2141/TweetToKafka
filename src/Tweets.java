public class Tweets
{
	private String createdAt, user, tweet, rating;

	public Tweets(String user, String tweet, String createdAt)
	{
		setUser(user);
		setTweet(tweet);
		setCreatedAt(createdAt);
//		setRating(rating);
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
	public String getRating()
	{
		return rating;
	}
	public void setRating(String rating)
	{
		this.rating = rating;
	}
}