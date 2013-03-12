package linegroup3.tweetstream.io.output;

import linegroup3.tweetstream.event.OnlineEvent;

public interface Cache {
	
	public String getId();
	
	public void put(String id, OnlineEvent event);
	
	public void update(String id, OnlineEvent event);

}
