package linegroup3.tweetstream.io.input;

import org.json.JSONObject;

public interface FilterTweet {
	boolean filterOut(JSONObject tweet);
	boolean filterOut(String content);
}
