package linegroup3.tweetstream.io.output;


public interface TweetMatch {
	public boolean match(String tweet, String[] keywords);
}
