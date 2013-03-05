package linegroup3.tweetstream.io.input.test;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import linegroup3.tweetstream.io.input.BufferManager;
import linegroup3.tweetstream.io.input.TweetExtractor;

import org.json.JSONObject;


public class BufferManagerTest {
	
	public static void main(String[] args){
		BlockingQueue<List<JSONObject>> queue = new LinkedBlockingQueue<List<JSONObject>>();
		
		BufferManager bufManager = new BufferManager();
		bufManager.setQueue(queue);
		
		new Thread(bufManager).start();
		
		while(true){
			System.out.println("---------------------------------");
			try {
				List<JSONObject> tweets = queue.take();
				if(tweets != null){
					for(JSONObject tweet : tweets){
						System.out.println(TweetExtractor.getTime(tweet) + "\t" + TweetExtractor.getId(tweet) + "\t" + TweetExtractor.getContent(tweet));
					}
					System.out.println("In all : " + tweets.size());
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}	

}
