package linegroup3.tweetstream.io.input;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.json.JSONException;
import org.json.JSONObject;

public class TweetExtractor {

	public static Timestamp getTime(JSONObject tweet) throws ParseException, JSONException{
		DateFormat utcDf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcDf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Timestamp t = new Timestamp(utcDf.parse(tweet.getString("publishedTimeGMTStr")).getTime());
		return t;
	}
	
	public static String getContent(JSONObject tweet) throws JSONException{
		return tweet.getString("content");
	}
	
	public static long getId(JSONObject tweet) throws JSONException{
		return tweet.getLong("statusId");
	}
	
}
