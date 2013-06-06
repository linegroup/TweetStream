package linegroup3.tweetstream.rt2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.json.JSONObject;




import linegroup3.common.Config;
import linegroup3.tweetstream.io.input.FetchTweets;
import linegroup3.tweetstream.io.input.FetcherMS;
import linegroup3.tweetstream.io.input.TweetExtractor;
import linegroup3.tweetstream.io.output.CacheAgent;
import linegroup3.tweetstream.io.output.RedisCache;
import linegroup3.tweetstream.postprocess.TokenizeTweet;
import linegroup3.tweetstream.preparedata.HashFamily;
import linegroup3.tweetstream.rt2.sket.Estimator;
import linegroup3.tweetstream.rt2.sket.History;
import linegroup3.tweetstream.rt2.sket.Pair;
import linegroup3.tweetstream.rt2.sket.Sketch;
import linegroup3.tweetstream.workers.InferenceUnit;
import linegroup3.tweetstream.workers.InferenceWorker;

public class RTProcessOffline {
	static final long oneDayLong = 24 * 60 * 60 * 1000; // (ms)
	
	/////// SET H & N HERE !!!
	static final int H = 5;
	static final int N = Config.N;
		
	private Timestamp DETECT_T = null;
	private static final double THRESHOLD_D_V = Config.THRESHOLD_D_V;
	private static final double THRESHOLD_D_A = Config.THRESHOLD_D_A;
	
	
	private static final int THREAD_POOL_SIZE = 2 * H;
	private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	
	private int CYCLE = 24*60; // minutes
	private int INTERVAL = 10; // minutes
	//private int MAX_QUEUE_SIZE = (CYCLE / INTERVAL) + 3; // interval(s)
	//private Sketch[] sketchQueue = new Sketch[MAX_QUEUE_SIZE];
	//private int head = 0;
	//private int tail = 0;
	
	private ActiveTerm2 activeTerms = new ActiveTerm2();
	
	public RTProcessOffline(){		
		//for(int i = 0; i < MAX_QUEUE_SIZE; i ++){
		//	sketchQueue[i] = new Sketch();
		//}
	}
	
	private Sketch currentSketch = null;
	
	
	private BlockingQueue<InferenceUnit> queueInference = new LinkedBlockingQueue<InferenceUnit>(25);
	private BlockingQueue<List<JSONObject>> queueTweets = new LinkedBlockingQueue<List<JSONObject>>(25);
	
	
	private History history = new History(INTERVAL);
	
	public void runTime(Timestamp dt) {
		history.process();
		history.print();

		InferenceWorker inferWorker = new InferenceWorker(queueInference);
		new Thread(inferWorker).start();
		
		//CacheAgent.set(new RedisCache(new FetcherMS()));
		
		new Thread(new Runnable(){

			@Override
			public void run() {
				FetchTweets fetcher = new FetcherMS(Config.FetcherMS_start, Config.FetcherMS_end);
				List<JSONObject> tweets = null;
				while((tweets = fetcher.fetch()) != null){
					try {
						queueTweets.put(tweets);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				System.out.println("\n\n----------------------\nfinished");
			}}).start();
		
		if(dt == null){
			DETECT_T = new Timestamp(System.currentTimeMillis() + 2 * 24 * 60 * 60 * 1000);
		}else{
			DETECT_T = dt;
		}
		
		
		StopWords.initialize();

		Timestamp one_min_after_lastTime = new Timestamp(0);

		currentSketch = new Sketch();

		final Semaphore taskFinished = new Semaphore(0);

		while (true) {

			List<JSONObject> tweets = null;
			try {
				tweets = queueTweets.take();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			if(tweets == null)	continue;
			
			System.out.println("got " + tweets.size() + " tweets.");

			for (JSONObject tweet : tweets) {
				try {

					final Timestamp t = TweetExtractor.getTime(tweet);

					String content = TweetExtractor.getContent(tweet);

					List<String> terms = TokenizeTweet.tokenizeTweet(content);

					List<String> finalTerms = new LinkedList<String>();
					for (String term : terms) {
						if (!StopWords.isStopWord(term)) {
							finalTerms.add(term);
							activeTerms.active(term, t);
						}
					}

					double l = 0;
					ArrayList<TreeMap<Integer, Integer>> counter = new ArrayList<TreeMap<Integer, Integer>>(
							H);
					for (int h = 0; h < H; h++) {
						counter.add(new TreeMap<Integer, Integer>());
					}

					for (String term : finalTerms) {
						if (term.length() >= 1) {

							for (int h = 0; h < H; h++) {
								int bucket = HashFamily.hash(h, term);

								Integer count = counter.get(h).get(bucket);
								if (count == null) {
									counter.get(h).put(bucket, 1);
								} else {
									counter.get(h).put(bucket, count + 1);
								}
							}

							l++;
						}
					}

					if (l <= 1) {
						continue;
					}

					// //////////////////////////////////////////////

					double ds = 0;
					// //// for zero order

					ds = 1;
					currentSketch.zeroOrderPulse(t, ds);

					// //// for first order
					for (int h = 0; h < H; h++) {
						final ArrayList<TreeMap<Integer, Integer>> f_counter = counter;
						final int f_h = h;
						final double f_l = l;

						pool.execute(new Runnable() {

							@Override
							public void run() {
								for (Map.Entry<Integer, Integer> entry : f_counter
										.get(f_h).entrySet()) {
									int bucket = entry.getKey();
									int count = entry.getValue();

									double ds = count / f_l;

									currentSketch.firstOrderPulse(t, ds, f_h,
											bucket);
								}

								taskFinished.release();
							}

						});

					}

					// //// for second order
					for (int h = 0; h < H; h++) {
						final ArrayList<TreeMap<Integer, Integer>> f_counter = counter;
						final int f_h = h;
						final double f_l = l;

						pool.execute(new Runnable() {

							@Override
							public void run() {
								for (Map.Entry<Integer, Integer> entry_i : f_counter
										.get(f_h).entrySet()) {
									int bucket_i = entry_i.getKey();
									int count_i = entry_i.getValue();

									for (Map.Entry<Integer, Integer> entry_j : f_counter
											.get(f_h).entrySet()) {
										int bucket_j = entry_j.getKey();
										int count_j = entry_j.getValue();

										double ds = 0;

										if (bucket_i == bucket_j) {
											ds = count_i * (count_i - 1);
										} else {
											ds = count_i * count_j;
										}
										ds /= f_l * (f_l - 1);

										currentSketch.secondOrderPulse(t, ds,
												f_h, bucket_i, bucket_j);

									}

								}

								taskFinished.release();
							}

						});

					}

					// ///////// synchronize
					try {
						taskFinished.acquire(2 * H);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					// //////////////// change observing time //////////////
					currentSketch.observe(t);

					// /////// write speed
					// final Pair speed = currentSketch.zeroOrder.get(t);
					// speedLogWrite(t, speed.v, speed.a);

					// ///// for difference
					if (t.after(DETECT_T)) {

						try {
							
							final Pair pair = history
									.zeroOrderDiff(currentSketch);
							// dspeedLogWrite(t, pair.v, pair.a);

							if (!t.before(one_min_after_lastTime)
									&& pair.a >= THRESHOLD_D_A
									&& pair.v >= THRESHOLD_D_V) {
								putSketch(currentSketch);
								one_min_after_lastTime = new Timestamp(
										t.getTime() + 60000);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}

					}

					// ///// cache snapshot
					/*
					final long oneMinute = 60 * 1000;

					Timestamp lastone = null;
					if (head == tail) {
						currentSketch.copy(sketchQueue[tail]);
						tail++;
					} else {
						int index = (tail - 1) % MAX_QUEUE_SIZE;
						Sketch lastSketch = sketchQueue[index];
						lastone = lastSketch.getTime();
						if (t.getTime() == lastone.getTime()) {
							currentSketch.copy(sketchQueue[index]);
						}
						if (t.getTime() - lastone.getTime() >= INTERVAL
								* oneMinute) {
							if (tail - head == MAX_QUEUE_SIZE) {
								head++;
							}
							currentSketch.copy(sketchQueue[tail
									% MAX_QUEUE_SIZE]);
							tail++;
						}
					}
					*/

				} catch (Exception e) {
					e.printStackTrace();
				}

			}

		}

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
	
	private void putSketch(Sketch sketch) throws Exception{

		InferenceUnit unit = new InferenceUnit();

		unit.currentTime = sketch.getTime();
		unit.zeroOrderDiff = history.zeroOrderDiff(sketch);
		unit.firstOrderDiff = history.firstOrderDiff(sketch);
		unit.secondOrderDiff = history.secondOrderDiff(sketch);

		unit.activeTerms = new LinkedList<String>();
		for(String term : activeTerms.activeTerms()){
			unit.activeTerms.add(term);
		}
			
		queueInference.put(unit);
	}
	

	/*
	private void trackFirstOrder(String dir){
		BufferedWriter out_V, out_A;
		for(int h = 0; h < H; h ++)
		try {
			out_V = new BufferedWriter(new FileWriter(dir + "/trace_V_" + h + ".txt"));
			out_A = new BufferedWriter(new FileWriter(dir + "/trace_A_" + h + ".txt"));
			
			for(int n = 0; n < N; n ++){
				int index = head;
				while(index != tail){
					Sketch sketch = sketchQueue[index % MAX_QUEUE_SIZE];
					Timestamp t = sketch.getTime();
					Pair pair = sketch.firstOrder[h][n].get(t);
					out_V.write("\t" + pair.v);
					out_A.write("\t" + pair.a);
					
					index ++;
				}
				out_V.write("\n");
				out_A.write("\n");
			}
			
			out_V.close();
			out_A.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}*/
}
