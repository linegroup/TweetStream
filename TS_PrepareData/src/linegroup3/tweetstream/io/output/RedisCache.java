package linegroup3.tweetstream.io.output;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import linegroup3.tweetstream.event.OnlineEvent;
import linegroup3.tweetstream.io.input.ReadTweets;
import linegroup3.tweetstream.io.input.TweetExtractor;

public class RedisCache implements Cache {
	private final String REDIS_HOST = "10.0.106.64";
	private final int REDIS_PORT = 6379;
	
	private Jedis jd = new Jedis(REDIS_HOST, REDIS_PORT);
	
	private final String KEY_EVENT_PREFIX = "twitter:sg:event:test:"; // !!!!!! test
	private final String KEY_EVENT_LATEST_ID = KEY_EVENT_PREFIX + "nextId";
	private final String KEY_EVENT_IDS = KEY_EVENT_PREFIX + "ids";
	private final String KEY_EVENT_TIMESTAMPS = KEY_EVENT_PREFIX + "timestamps";
	private final String KEY_EVENT_POSTFIX_REL_TWEETS = ":rel-tweets";
	
	private TweetMatch tMatcher = new Matcher();
	private ReadTweets tReader = null;
	public RedisCache(ReadTweets tReader){
		this.tReader = tReader;
	}

	@Override
	public String getId() {
		jd.incr(KEY_EVENT_LATEST_ID);
		return jd.get(KEY_EVENT_LATEST_ID);
	}

	@Override
	public void put(String id, OnlineEvent event) {
		System.out.println("put:" + "\t" + event.toString());
		
		jd.rpush(KEY_EVENT_IDS, id); 
		jd.rpush(KEY_EVENT_TIMESTAMPS, event.getStart().toString());
		
		String key = KEY_EVENT_PREFIX + event.getId();
		jd.set(key, event2json(event).toString());
	}

	@Override
	public void update(String id, OnlineEvent event) {
		System.out.println("update:" + "\t" + event.toString());
		
		pushTweets(event);
		
		String key = KEY_EVENT_PREFIX + event.getId();
		jd.set(key, event2json(event).toString());
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
	
	private void pushTweets(OnlineEvent event){
		Timestamp t = event.getEnd();
		int MINUTES = 15;
		
		String key = KEY_EVENT_PREFIX + event.getId() + KEY_EVENT_POSTFIX_REL_TWEETS;
		
		List<JSONObject> tweets = getTweetsFormRedis(key);
		Set<Long> idSet = new TreeSet<Long>();
		for(JSONObject tweet : tweets){
			try {
				long id = TweetExtractor.getId(tweet);
				idSet.add(id);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		List<String> kwList = event.getKeywords();
		String[] keywords = new String[kwList.size()];
		int i = 0;
		for(String keyword : kwList){
			keywords[i] = keyword;
			i ++;
		}
		
		List<JSONObject> newTweets = tReader.read(t, MINUTES);
		for(JSONObject tweet : newTweets){
			try {
				long id = TweetExtractor.getId(tweet);
				if(!idSet.contains(id)){
					String content = TweetExtractor.getContent(tweet);
					if(tMatcher.match(content, keywords)){
						tweets.add(tweet);
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		putTweetsToRedis(key, tweets);
	}
	
	private List<JSONObject> getTweetsFormRedis(String key){
		List<JSONObject> ret = new LinkedList<JSONObject>();
		String tweets = jd.get(key);
		if(tweets == null) return ret;
		try {
			JSONArray array = new JSONArray(tweets);
			for(int i = 0; i < array.length(); i ++){
				ret.add(array.getJSONObject(i));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	private void putTweetsToRedis(String key, List<JSONObject> tweets){
		JSONArray array = new JSONArray(tweets);
		jd.set(key, array.toString());
	}
	
	public void clear(){
		System.out.println("\nDeleting the following event-related keys...");
		for(String k: jd.keys(KEY_EVENT_PREFIX + "*")){
			System.out.println("Deleting " + k);
			jd.del(k);
		}
	}
	
	public void print(){
		System.out.println("\nPrinting the following event-related keys...");
		for(String k: jd.keys(KEY_EVENT_PREFIX + "*")){
			System.out.println(k);
		}
	}

}
