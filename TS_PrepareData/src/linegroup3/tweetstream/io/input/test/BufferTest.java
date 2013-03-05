package linegroup3.tweetstream.io.input.test;

import org.json.JSONException;
import org.json.JSONObject;

import linegroup3.tweetstream.io.input.Buffer;
import linegroup3.tweetstream.io.input.ProcessObject;

public class BufferTest implements ProcessObject {
	
	public static void main(String[] args) throws JSONException{
		Buffer buffer = new Buffer(3);
		
		for(int i = 0; i < 10; i ++){
			JSONObject object = new JSONObject();
			object.put("id", i);
			System.out.println(buffer.put(object));
		}
		
		buffer.scan(new BufferTest());
	}

	@Override
	public void process(JSONObject object) {
		System.out.println(object);
	}
	
}
