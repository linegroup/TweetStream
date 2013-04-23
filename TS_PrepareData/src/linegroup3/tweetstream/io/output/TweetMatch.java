package linegroup3.tweetstream.io.output;

import linegroup3.tweetstream.event.OnlineEvent;


public interface TweetMatch {
	public boolean match(String tweet, String[] keywords);
	//public boolean match(String tweet, OnlineEvent event);
}
