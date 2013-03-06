package linegroup3.tweetstream.io.input;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

import aek.hbasepoller.hbase.Tweet;
import aek.hbasepoller.poller.HBaseTweetFetcher;


public class Fetcher implements FetchTweets{
	
	private static HBaseTweetFetcher fetcher = null;
	private static String hTableName = "plr_sg_tweet_live";
	private static int lagMinute = 0;
	private static int intervalMinute = 1;
	
	static{
		try {
			fetcher = new HBaseTweetFetcher(hTableName, lagMinute, intervalMinute);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public List<JSONObject> fetch() {
		try {
			fetcher.fetch();
			Gson gs = new Gson();
			Tweet[] tweets = gs.fromJson(fetcher.getCurFetchJSONString(), Tweet[].class);
			
			List<JSONObject> ret = new LinkedList<JSONObject>();
			for(Tweet tweet : tweets){
				String str = gs.toJson(tweet);
				JSONObject obj = new JSONObject(str);
				ret.add(obj);
			}
			
			return ret;
			
		} catch (IOException ie) {
			ie.printStackTrace();
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return null;
	}
	
	/*public List<JSONObject> fetch() {
		final String uri = "http://research.larc.smu.edu.sg:8080/PalanteerDevApi/rest/v1/tweets/search";
		final String q = null;
		final int delayMin = 1;
		final int intervalMin = 1;
		int pageNo = 1;
		
		RealtimeAPITweetFetcher fetcher;
		try {
			fetcher = new RealtimeAPITweetFetcher(uri, q, delayMin, intervalMin);
			return fetcher.fetch(new ArrayList<JSONObject>(), pageNo);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null; 
	}*/

}
