package linegroup3.tweetstream.io.input;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import test.RealtimeAPITweetFetcher;

public class Fetcher implements FetchTweets{

	@Override
	public List<JSONObject> fetch() {
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
	}

}
