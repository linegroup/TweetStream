package linegroup3.tweetstream.io.output;

import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import linegroup3.tweetstream.event.OnlineEvent;

public class RedisCache implements Cache {
	private final String REDIS_HOST = "10.0.106.64";
	private final int REDIS_PORT = 6379;
	
	private Jedis jd = new Jedis(REDIS_HOST, REDIS_PORT);
	
	private final String KEY_EVENT_PREFIX = "twitter:sg:event:test:"; // !!!!!! test
	private final String KEY_EVENT_LATEST_ID = KEY_EVENT_PREFIX + "nextId";
	private final String KEY_EVENT_IDS = KEY_EVENT_PREFIX + "ids";
	private final String KEY_EVENT_TIMESTAMPS = KEY_EVENT_PREFIX + "timestamps";
	private final String KEY_EVENT_POSTFIX_REL_TWEETS = ":rel-tweets";

	@Override
	public String getId() {
		jd.incr(KEY_EVENT_LATEST_ID);
		return jd.get(KEY_EVENT_LATEST_ID);
	}

	@Override
	public void put(String id, OnlineEvent event) {
		jd.rpush(KEY_EVENT_IDS, id); 
		jd.rpush(KEY_EVENT_TIMESTAMPS, event.getStart().toString());
		
		jd.set(id, event2json(event).toString());
	}

	@Override
	public void update(String id, OnlineEvent event) {
		jd.set(id, event2json(event).toString());
	}
	
	private final int SPAN_THRESHOLD = 3;
	private JSONObject event2json(OnlineEvent event){
		JSONObject ret = new JSONObject();
		
		try {
			ret.put("time", event.getStart().toString());
			ret.put("lastDetection", event.getEnd().toString());
			long span = (event.getEnd().getTime() - event.getStart().getTime()) / (60 * 1000);
			ret.put("type", span >= SPAN_THRESHOLD ? 1:0);
			ret.put("keywords", event.getKeywordsStr());
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return ret;
	}
	
	public void clear(){
		System.out.println("\nDeleting the following event-related keys...");
		for(String k: jd.keys(KEY_EVENT_PREFIX + "*")){
			System.out.println("Deleting " + k);
			jd.del(k);
		}
	}

}
