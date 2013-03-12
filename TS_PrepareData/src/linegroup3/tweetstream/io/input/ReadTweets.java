package linegroup3.tweetstream.io.input;

import java.sql.Timestamp;
import java.util.List;

import org.json.JSONObject;

public interface ReadTweets {
	public List<JSONObject> read(Timestamp t, int minutes);
}
