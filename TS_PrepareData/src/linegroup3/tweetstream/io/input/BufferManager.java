package linegroup3.tweetstream.io.input;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.json.JSONException;
import org.json.JSONObject;


public class BufferManager implements Runnable, ReadTweets {
	
	public BufferManager(){
		setFetcher(new Fetcher());
		setFilter(new FilterTweet(){

			@Override
			public boolean filterOut(JSONObject tweet) {
				return false;
			}});
	}
	
	public void setQueue(BlockingQueue<List<JSONObject>> queue){
		this.queue = queue;
	}
	
	public void setFetcher(FetchTweets fetcher){
		this.fetcher = fetcher;
	}
	
	public void setFilter(FilterTweet filter){
		this.filter = filter;
	}
	
	@Override
	public void run() {
		timer.schedule(new TimerTask(){
			
			@Override
			public void run() {
				read();
			}}, 0, INTERVAL * 60 * 1000);
		
		while(true){
			write();
			
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void write(){
		
		List<JSONObject> tweets = fetcher.fetch();
		
		for(JSONObject tweet : tweets){
			try {
				long id = tweet.getLong("statusId");
				
				if(!ids.contains(id)){
					ids.add(id);
					JSONObject oldTweet = buffer.put(tweet);
					if(oldTweet != null){
						long oldId = TweetExtractor.getId(oldTweet);
						ids.remove(oldId);
					}
				}
				
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public List<JSONObject> read(Timestamp t, int minutes) { // (start, end]
		final Timestamp start = new Timestamp(t.getTime() - minutes * 60 * 1000);
		final Timestamp end = new Timestamp(t.getTime());
		final TreeMap<Timestamp, List<JSONObject>> map = new TreeMap<Timestamp, List<JSONObject>>();
		
		buffer.scan(new ProcessObject(){

			@Override
			public void process(JSONObject tweet) {
				try {
					Timestamp t = TweetExtractor.getTime(tweet);
					if(t.after(start) && !t.after(end)){
						if(!filter.filterOut(tweet)){
							
							List<JSONObject> list = map.get(t);
							if(list == null){
								list = new LinkedList<JSONObject>();
								map.put(t, list);
							}
							list.add(tweet);
						}
					}			
					
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			
		});
		
		List<JSONObject> output = new LinkedList<JSONObject>();
		for(Map.Entry<Timestamp, List<JSONObject>> entry : map.entrySet()){
			output.addAll(entry.getValue());
		}
		
		return output;
	}
	
	private void read(){ // must be finished in INTERVAL minute(s)
		final Timestamp start = new Timestamp(System.currentTimeMillis() - DELAY * 60 *1000);
		final Timestamp end = new Timestamp(start.getTime() + INTERVAL * 60 * 1000);
		
		final TreeMap<Timestamp, List<JSONObject>> map = new TreeMap<Timestamp, List<JSONObject>>();
		
		buffer.scan(new ProcessObject(){

			@Override
			public void process(JSONObject tweet) {
				try {
					Timestamp t = TweetExtractor.getTime(tweet);
					if(t.after(start) && !t.after(end)){
						if(!filter.filterOut(tweet)){
							
							List<JSONObject> list = map.get(t);
							if(list == null){
								list = new LinkedList<JSONObject>();
								map.put(t, list);
							}
							list.add(tweet);
						}
					}			
					
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			
		});
		
		List<JSONObject> output = new LinkedList<JSONObject>();
		for(Map.Entry<Timestamp, List<JSONObject>> entry : map.entrySet()){
			output.addAll(entry.getValue());
		}
		
		try {
			queue.put(output);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	

	
	private int DELAY = 2; // process the tweets DELAY minutes ago in buffer
	
	private int INTERVAL = 1; // process every INTERVAL minute(s)
	
	private Buffer buffer = new Buffer(30000);
	
	private HashSet<Long> ids = new HashSet<Long>();
	
	private BlockingQueue<List<JSONObject>> queue = null;
	
	private FetchTweets fetcher = null;
	
	private FilterTweet filter = null;

	private Timer timer = new Timer();

}
