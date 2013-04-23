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
import linegroup3.tweetstream.postprocess.TokenizeTweet;
import linegroup3.tweetstream.rt2.StopWords;

public class RedisCache implements Cache {
	private final String REDIS_HOST = "10.0.106.64";
	private final int REDIS_PORT = 6379;
	
	private Jedis jd = new Jedis(REDIS_HOST, REDIS_PORT);
	
	private final String KEY_EVENT_CHANNEL = "twitter:sg:event:test2";
	private final String KEY_EVENT_PREFIX = KEY_EVENT_CHANNEL + ":";
	private final String KEY_EVENT_LATEST_ID = KEY_EVENT_PREFIX + "nextId";
	private final String KEY_EVENT_IDS = KEY_EVENT_PREFIX + "ids";
	private final String KEY_EVENT_TIMESTAMPS = KEY_EVENT_PREFIX + "timestamps";
	private final String KEY_EVENT_POSTFIX_REL_TWEETS = ":rel-tweets";
	
	private TweetMatch tMatcher = new Matcher();
	private ReadTweets tReader = null;
	
	private class AdditionalInfo{
		private int numTweets = 0;
		private int numGeoTweets = 0;
		private int numUsers = 0;
		private int numGeoUsers = 0;
		private double RTrate = 0.0;
		public int getNumTweets(){return numTweets;}
		public int getNumGeoTweets(){return numGeoTweets;}
		public int getNumUsers(){return numUsers;}
		public int getNumGeoUsers(){return numGeoUsers;}
		public double getRTrate(){return RTrate;}
		public void setNumTweets(int num){numTweets = num;}
		public void setNumGeoTweets(int num){numGeoTweets = num;}
		public void setNumUsers(int num){numUsers = num;}
		public void setNumGeoUsers(int num){numGeoUsers = num;}
		public void setRTrate(double rt){RTrate = rt;}
	}
	
	
	
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
		
		jd.publish(KEY_EVENT_CHANNEL, event.getId());
	}

	@Override
	public void update(String id, OnlineEvent event) {
		System.out.println("update:" + "\t" + event.toString());
		
		AdditionalInfo adInfo = new AdditionalInfo();
		pushTweets(event, adInfo);
		
		String key = KEY_EVENT_PREFIX + event.getId();
		jd.set(key, event2json(event, adInfo).toString());
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
	
	private JSONObject event2json(OnlineEvent event, AdditionalInfo adInfo){
		JSONObject ret = new JSONObject();
		
		try {
			ret.put("time", event.getStart().toString());
			ret.put("lastDetection", event.getEnd().toString());
			ret.put("type", type(event, adInfo));
			ret.put("keywords", event.getKeywordsStr());
			
			ret.put("numTweets", adInfo.getNumTweets());
			ret.put("numUsers", adInfo.getNumUsers());
			ret.put("numGeoTweets", adInfo.getNumGeoTweets());
			ret.put("numGeoUsers", adInfo.getNumGeoUsers());
			ret.put("RTrate", adInfo.getRTrate());
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return ret;
	}
	
	private int type(OnlineEvent event, AdditionalInfo adInfo){
		int numTweets = adInfo.getNumTweets();
		int numUsers= adInfo.getNumUsers();
		
		List<String> list = event.getKeywords();
		if(list.size() == 1){
			if(list.get(0).contentEquals("morning")){
				return 0;
			}
		}
		
		if(numTweets > 1500) return 1;
		
		if(adInfo.getRTrate() >= 0.95) return 0;
		
		if(numTweets >= 500 && numUsers >= 100) return 1;
			
		double r = numTweets;
		if(numUsers >0){
			r /= numUsers;
		}
		if(numUsers >= 100 && r <= 1.5) return 1;
		
		if(numTweets < 200 || numUsers < 5) return 0;
		
		long span = (event.getEnd().getTime() - event.getStart().getTime()) / (60 * 1000);
		return (span >= SPAN_THRESHOLD ? 1:0);
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
		event.beginParseTweets();
		tweets2event(event, tweets);
	}
	
	private void pushTweets(OnlineEvent event, AdditionalInfo adInfo){
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
		
		//////////////////////////////////////////////////////
		Set<Long> tweetsSet = new TreeSet<Long>();
		Set<Long> usersSet = new TreeSet<Long>();
		Set<Long> geoTweetsSet = new TreeSet<Long>();
		Set<Long> geoUsersSet = new TreeSet<Long>();
		double rtCnt = 0.0;
		for(JSONObject tweet : tweets){
			try{
				long id = TweetExtractor.getId(tweet);
				long userId = TweetExtractor.getUserId(tweet);
				String geo = TweetExtractor.getGeo(tweet);
				String content = TweetExtractor.getContent(tweet);
				
				if(content.startsWith("RT @")){
					rtCnt += 1;
				}
				
				tweetsSet.add(id);
				usersSet.add(userId);
				
				if(geo != null){
					geoTweetsSet.add(id);
					geoUsersSet.add(userId);
				}
			} catch(JSONException je){
				je.printStackTrace();
			}		
		}
		adInfo.setNumTweets(tweetsSet.size());
		adInfo.setNumUsers(usersSet.size());
		adInfo.setNumGeoTweets(geoTweetsSet.size());
		adInfo.setNumGeoUsers(geoUsersSet.size());
		if(tweets.size() == 0){
			adInfo.setRTrate(0);
		}else{
			adInfo.setRTrate(rtCnt / tweets.size());
		}
		//////////////////////////////////////////////////////
		
		putTweetsToRedis(key, tweets);
		event.beginParseTweets();
		tweets2event(event, tweets);
	}
	
	private void tweets2event(OnlineEvent event, List<JSONObject> tweets){
		int n = tweets.size();
		
		for(JSONObject tweet : tweets){
			try {
				String content = TweetExtractor.getContent(tweet);
				List<String> terms = TokenizeTweet.tokenizeTweet(content);

				List<String> finalTerms = new LinkedList<String>();
				for (String term : terms) {
					if (!StopWords.isStopWord(term)) {
						finalTerms.add(term);
					}
				}
				
				if(finalTerms.size() == 0) continue;
				
				for(String term : finalTerms){
					event.parseTweets(term, 1.0/(finalTerms.size() * n));
				}
				
				
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
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
