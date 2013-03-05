package test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Fetch tweets published between now and (now-fetchIntervalMin) UTC timestamps 
 * via the PalanteerDev RESTful API in parallel.
 * By default, 5 concurrent requests are made to the API
 * @author aek
 *
 */
public class RealtimeAPITweetFetcher {

	private final int NUM_FETCHER_THREADS = 10;
	private final int NUM_FETCHERS = 5;
	private final int MAX_RESP_RECORDS = 1000; // Max response records per API request.
	private ExecutorService fetcherExec = Executors.newFixedThreadPool(NUM_FETCHER_THREADS);
	private String resourceURI = null;
	private String query = null;
	private int delayMin = 1;
	private int fetchIntervalMin = 1;

	public RealtimeAPITweetFetcher(String resourceURI, String query, int delayMin, int fetchIntervalMin) throws Exception{
		if(resourceURI == null){
			throw new Exception("Resource URI cannot be null.");
		}
		if(delayMin<0){
			throw new Exception("Delay minute cannot be negative");
		}
		if(fetchIntervalMin<0){
			throw new Exception("Fetch Interval minute cannot be negative");
		}
		this.resourceURI = resourceURI;
		this.delayMin = delayMin;
		this.fetchIntervalMin = fetchIntervalMin;
	}

	/**
	 * Recursively fetch tweets
	 * @param list Starting list to store the output
	 * @param pageNo Starting pageNo parameter
	 * @return A list of JSONObject representing the tweet data. 
	 * JSONObject's keys for /tweets/search's data response body:
	 * - statusId
	 * - userId
	 * - screenName
	 * - publishedTimeGMTStr
	 * - content
	 * See the PalanteerAPI doc for more information.
	 */
	@SuppressWarnings("unchecked")
	public List<JSONObject> fetch(List<JSONObject> list, int pageNo){
		Date end = new Date(new Date().getTime() - (this.delayMin*60*1000));
		Date start = new Date(end.getTime() - (this.fetchIntervalMin*60*1000));
		DateFormat utcDf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00'Z'");
		utcDf.setTimeZone(TimeZone.getTimeZone("UTC"));
		List<Future<JSONArray>> futures = new ArrayList<Future<JSONArray>>();

		for (int i = 0; i < NUM_FETCHERS; i++) {
			String uriStr = this.resourceURI + "?start=" + utcDf.format(start)
					+ "&end=" + utcDf.format(end) + "&page=" + (pageNo++);
			if(this.query == null) uriStr += "&q=*";
			else uriStr += "&q=" + this.query;
			TweetFetcher fetcher = new TweetFetcher(uriStr);
			futures.add(fetcherExec.submit(fetcher));
		}

		fetcherExec.shutdown();

		while(!fetcherExec.isTerminated()){ // wait until all threads finish
		}

		for(Future<JSONArray> i: futures){

			try {
				JSONArray ja = i.get();

				for(int j=0; j<ja.length(); j++){
					list.add((JSONObject) ja.getJSONObject(j));
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

		// Stop when the response size is less the expected max size
		if(list.size() < (this.MAX_RESP_RECORDS*this.NUM_FETCHERS)){
			return list;
		}
		else{
			fetch(list, pageNo);
		}
		return null;
	}

	// Test fetching tweets and printing out the content with timestamp
	public static void main(String[] args){
		final String uri = "http://research.larc.smu.edu.sg:8080/PalanteerDevApi/rest/v1/tweets/search";
		final String q = null;
		final int delayMin = 1;
		final int intervalMin = 1;
		int pageNo = 1;
		for(int r = 0; r < 3; r ++){
		System.out.println("round " + r + "---------------------------------------");				
			
		RealtimeAPITweetFetcher fetcher = null;
		try {
			fetcher = new RealtimeAPITweetFetcher(uri, q, delayMin, intervalMin);
			List<JSONObject> twList = fetcher.fetch(new ArrayList<JSONObject>(), pageNo); 

			for(JSONObject i: twList){
				DateFormat utcDf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
				utcDf.setTimeZone(TimeZone.getTimeZone("UTC"));
				Timestamp t = new Timestamp(utcDf.parse(i.getString("publishedTimeGMTStr")).getTime());
				System.out.println(t +"\t"+ i.getLong("statusId") + "\t" +  i.getString("content"));
			}

			System.out.println("Total tweets = "+twList.size());

		} catch (Exception e) {
			e.printStackTrace();
		}
		}

	}
}

class TweetFetcher implements Callable {

	private String urlStr = null;

	public TweetFetcher(String urlStr) {
		this.urlStr = urlStr;
	}

	public JSONArray call() {
		URI uri = null;

		try {
			uri = new URI(this.urlStr);
			JSONTokener tok = new JSONTokener(uri.toURL().openStream());
			JSONObject root = new JSONObject(tok);
			return root.getJSONArray("data");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

}