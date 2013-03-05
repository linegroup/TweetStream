package linegroup3.tweetstream.io.input;

import java.util.List;

import org.json.JSONObject;

public interface FetchTweets {
	public List<JSONObject> fetch();
}
