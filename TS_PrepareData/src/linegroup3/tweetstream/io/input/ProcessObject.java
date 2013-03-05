package linegroup3.tweetstream.io.input;

import org.json.JSONObject;

public interface ProcessObject {
	public void process(JSONObject object);
}
