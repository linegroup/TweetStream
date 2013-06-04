package linegroup3.tweetstream.io.input;

import linegroup3.common.Config;

import org.json.JSONException;
import org.json.JSONObject;

public class FilterPopUsers implements FilterTweet{

	@Override
	public boolean filterOut(JSONObject tweet) {
		String content;
		try {
			content = TweetExtractor.getContent(tweet);
			return filterOut(content);
		} catch (JSONException e) {
			e.printStackTrace();
			
		}
		return false;
	}

	@Override
	public boolean filterOut(String content) {
		if(!Config.include_RT){
			if(content.startsWith("RT @"))	return true;
		}
		
		String lowCase = content.toLowerCase();
		
		
		if(lowCase.contains("@kingsleyyy")) 
			return true;
		if(lowCase.contains("@girlposts")) 
			return true;
		if(lowCase.contains("@freddyamazin")) 
			return true;
		if(lowCase.contains("@comedyposts")) 
			return true;
		if(lowCase.contains("@wethinkforgirls")) 
			return true;
		if(lowCase.contains("@girlnotes")) 
			return true;
		if(lowCase.contains("@schoolprobiems")) 
			return true;
		if(lowCase.contains("@comedypics")) 
			return true;
		return false;
	}

}
