package linegroup3.tweetstream.postprocess;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import cmu.arktweetnlp.Twokenize;

import linegroup3.tweetstream.preparedata.HashFamily;
import linegroup3.tweetstream.rt2.ActiveTerm;
import linegroup3.tweetstream.rt2.StopWords;
import linegroup3.tweetstream.rt2.sket.Pair;
import linegroup3.tweetstream.rt2.sket.Sketch;

public class SketchEfficiencyTest {
	/////// SET H & K HERE !!!
	static final int H = 5;
	static final int N = 200;
	
	static final long oneDayLong = 24 * 60 * 60 * 1000; // (ms)
	
	static private Connection conn = null;
	
	static {
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://10.4.8.16/tweetstream?"
							+ "user=root&password=123583");

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

			conn = null;

		}
	}
	
	private static final int THREAD_POOL_SIZE = 5 ;
	private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	
	private Sketch currentSketch = null;
	
	private ActiveTerm activeTerms = new ActiveTerm();
	
	private ArrayList<TreeMap<Integer, Integer>> g_counter;
	private double g_l;
	private Timestamp g_t;
	
	public void runTime(Timestamp start, Timestamp end) throws IOException{	
		long totalNumOfTweets = 0;
		long sketchTime = 0;
		
		StopWords.initialize();
		
		Timestamp next = new Timestamp(start.getTime()+oneDayLong);
				
		currentSketch = new Sketch();	
		
		final Semaphore taskFinished = new Semaphore(0);
		final Semaphore taskStarted = new Semaphore(0);
		
		
		
		
		LinkedList<ArrayList<TreeMap<Integer, Integer>>> counter_list = new LinkedList<ArrayList<TreeMap<Integer, Integer>>>();
		LinkedList<Double> l_list = new LinkedList<Double>();
		LinkedList<Timestamp> t_list = new LinkedList<Timestamp>();
		
		while(start.before(end)){
			System.out.println(new Timestamp(System.currentTimeMillis()) + "\tProcessing : " + start);  // print info
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream where t >= \'" + start + "\' and t < \'" + next +"\'";
				if (stmt.execute(sqlTxt)) {
					
					rs = stmt.getResultSet();
					while (rs.next()) {
						totalNumOfTweets ++;
						/// debug
						///long debug_T = System.currentTimeMillis();
						/////////////////////////////////
						long ct = System.currentTimeMillis();
						
												
						final Timestamp t = rs.getTimestamp("t");
						String tweet = rs.getString("tweet");
						
						tweet = decode(tweet);
						tweet = downcase(tweet);
						List<String> terms = tokenize(tweet);
						
						List<String> finalTerms = new LinkedList<String>();
						for(String term : terms){
							if(!StopWords.isStopWord(term)){
								finalTerms.add(term);
								//activeTerms
							}		
						}
						
						////// counting
						double l = 0; 
						ArrayList<TreeMap<Integer, Integer>> counter = new ArrayList<TreeMap<Integer, Integer>>(H);
						for(int h = 0; h < H; h ++){
							counter.add(new TreeMap<Integer, Integer>());
						}
						
						for(String term : finalTerms){
							if(term.length() >= 1){
								int id = Math.abs(term.hashCode());
															
								for(int h = 0; h < H; h ++){
									int bucket = HashFamily.hash(h, id);
									
									Integer count = counter.get(h).get(bucket);
									if(count == null){
										counter.get(h).put(bucket, 1);
									}else{
										counter.get(h).put(bucket, count + 1);
									}
								}
								
								l ++;
							}
						}
						
						if(l <= 1){
							/////////////////////////////// for debug
							//System.out.print("L i s less than 2!!!!!!!");
							//System.out.println(rs.getString("status_ID"));
							continue;
						}
						
						////////////////////////////////////////////////
						counter_list.add(counter);
						l_list.add(l);
						t_list.add(t);
						
					}
					
					//////////////////////////////////
					System.out.println("start processing sketch...");
					
					long ct = System.currentTimeMillis();
					

					
					while(!t_list.isEmpty()){
						
					g_t = t_list.pollFirst();
					g_l = l_list.pollFirst();
					g_counter = counter_list.pollFirst();
					
					lock();
					
					
					for (int h = 0; h < H; h++) {
						final int f_h = h;
						
						pool.execute(new Runnable(){

							@Override
							public void run() {
								//while(true){
								try {
									taskStarted.acquire();
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								
								double f_l = g_l;
								Timestamp t = g_t;
								ArrayList<TreeMap<Integer, Integer>> f_counter = g_counter;

								
								for (Map.Entry<Integer, Integer> entry : f_counter
										.get(f_h).entrySet()) {
									int bucket = entry.getKey();
									int count = entry.getValue();
									
									double ds = count / f_l;
									
									currentSketch.firstOrderPulse(t, ds, f_h, bucket);								
								}
								
								
								for (Map.Entry<Integer, Integer> entry_i : f_counter.get(f_h).entrySet()) {
									int bucket_i = entry_i.getKey();
									int count_i = entry_i.getValue();
									
									for(Map.Entry<Integer, Integer> entry_j : f_counter.get(f_h).entrySet()) {
										int bucket_j = entry_j.getKey();
										int count_j = entry_j.getValue();
										
										double ds = 0;
										
										if(bucket_i == bucket_j){
											ds = count_i*(count_i-1);		
										}else{
											ds = count_i*count_j;
										}
										ds /= f_l*(f_l-1);
										
										currentSketch.secondOrderPulse(t, ds, f_h, bucket_i, bucket_j);
										
									}
									
								}
								
								taskFinished.release();
								//}
							}
							
						});
					}
					
					taskStarted.release(H);
					
					
					double ds = 0;
					////// for zero order
					Timestamp t = g_t;
					ds = 1;
					currentSketch.zeroOrderPulse(t, ds);
					
					try {
						taskFinished.acquire(H);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					////////////////// change observing time //////////////
					currentSketch.observe(t);
					unlock();
					
					
					}
					
					ct = System.currentTimeMillis() - ct;
					sketchTime += ct;
					
					checkFirstOrder();/////// for debugging
	
				}
				
				

			} catch (SQLException ex) {
				// handle any errors
				System.out.println("SQLException: " + ex.getMessage());
				System.out.println("SQLState: " + ex.getSQLState());
				System.out.println("VendorError: " + ex.getErrorCode());

			} finally {
				// it is a good idea to release
				// resources in a finally{} block
				// in reverse-order of their creation
				// if they are no-longer needed
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException sqlEx) {
					} // ignore
					rs = null;
				}
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException sqlEx) {
					} // ignore
					stmt = null;
				}
			}
						
			start = next;
			next = new Timestamp(start.getTime()+oneDayLong);
			
		}
		
		System.out.println("total number of tweets is " + totalNumOfTweets);
		System.out.println("total time for sketch is " + sketchTime);
		
	}
	
	private static String decode(String tweet){
		return org.apache.commons.lang.StringEscapeUtils.unescapeHtml(tweet);
	}
	
	private static String downcase(String tweet) {
		return tweet.toLowerCase();
	}
	
	private List<String> tokenize(String tweet) {
		String str = tweet;
		str = str.replaceAll("\\.{10,}+", " ");
		List<String> terms = Twokenize.tokenize(str);

		/*
		final String regex = "\\p{Punct}+";
		for (String term : terms) {
			if (term.length() > 0 && term.length() <= 64
					&& !term.matches(regex)) {
				ret.add(term);
			}
		}
		*/

		return terms;

	}
	

	
	private void lock(){}
	
	private void unlock(){
		
	}
	
	
	private void checkFirstOrder(){
		//Sketch sketch = sketchQueue[(tail-1) % MAX_QUEUE_SIZE];
		Sketch sketch = currentSketch;
		Timestamp currentTime = sketch.getTime();
		System.out.println("Checking..." + currentTime);
		
		{
			Pair pair0 = sketch.zeroOrder.get(currentTime);
			for(int h = 0; h < H; h ++){
				double C_V = 0;
				double C_A = 0;
				for(int i = 0; i < N; i ++){
					Pair pair1 = sketch.firstOrder[h][i].get(currentTime);
					C_V += pair1.v;
					C_A += pair1.a;
				}
				if(Math.abs(C_V - pair0.v) > 1e-9) System.out.println("V Error!" + h + " " + (C_V - pair0.v) + " " + pair0.v);
				if(Math.abs(C_A - pair0.a) > 1e-9) System.out.println("A Error!" + h + " " + (C_A - pair0.a) + " " + pair0.a);
			}	
		}
		

	}
	
	
}
