package linegroup3.tweetstream.rt2.sket;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.json.JSONObject;

import linegroup3.common.Config;
import linegroup3.tweetstream.io.input.FetchTweets;
import linegroup3.tweetstream.io.input.FetcherMS;
import linegroup3.tweetstream.io.input.TweetExtractor;
import linegroup3.tweetstream.postprocess.TokenizeTweet;
import linegroup3.tweetstream.preparedata.HashFamily;
import linegroup3.tweetstream.rt2.StopWords;

public class History {
	private static int N = Config.N;
	private static int H = 5;
	
	private static final int THREAD_POOL_SIZE = 2 * H;
	private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	
	public History(int interval){
		T = (24*60) / interval;
		Interval = interval;
		
		count = new int[T];
		
		zeroOrderArray = new Pair[T];
		for(int t = 0; t < T; t ++){
			zeroOrderArray[t] = new Pair(0.0, 0.0);
		}
		
		firstOrderArray = new Pair[T][H][N];
		for(int t = 0; t < T; t ++){
			for(int h = 0; h < H; h ++){
				for(int n = 0; n < N; n ++){
					firstOrderArray[t][h][n] = new Pair(0.0, 0.0); 
				}
			}
		}
		
		secondOrderArray = new Pair[T][H][N][N];
		for(int t = 0; t < T; t ++){
			for(int h = 0; h < H; h ++){
				for(int n = 0; n < N; n ++){
					for(int n_ = 0; n_ < N; n_ ++){
						secondOrderArray[t][h][n][n_] = new Pair(0.0, 0.0); 
					}
				}
			}
		}
	}
	
	private int T = -1;
	private int Interval = 0;

	private int[] count = null;
	
	private Pair[] zeroOrderArray = null;
	private Pair[][][] firstOrderArray = null;
	private Pair[][][][] secondOrderArray = null;
	
	private Sketch currentSketch = null;
	
	public void process() {
		FetchTweets fetcher = new FetcherMS(Config.historyS, Config.historyE);
		List<JSONObject> tweets = null;

		Sketch lastSketch = new Sketch();
		Timestamp lastT = null;
		currentSketch = new Sketch();

		final Semaphore taskFinished = new Semaphore(0);

		while ((tweets = fetcher.fetch()) != null) {

			System.out.println("got " + tweets.size() + " tweets.");

			for (JSONObject tweet : tweets) {
				try {

					final Timestamp t = TweetExtractor.getTime(tweet);
					
					/////////////// process //////////////
					Timestamp midT = find(lastT, t);
					if(midT != null){
						currentSketch.copy(lastSketch);
					}
					//////////////////////////////////////

					String content = TweetExtractor.getContent(tweet);

					List<String> terms = TokenizeTweet.tokenizeTweet(content);

					List<String> finalTerms = new LinkedList<String>();
					for (String term : terms) {
						if (!StopWords.isStopWord(term)) {
							finalTerms.add(term);
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
					
					/////////////// process //////////////
					if(midT != null){
						if(!midT.before(Config.historyStart)){
							put(lastSketch, currentSketch, midT);
						}
					}
					lastT = t;
					//////////////////////////////////////
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				
			}
		}

		System.out.println("\n\n----------------------\nhistory finished");
	}
	
	private Timestamp find(Timestamp t1, Timestamp t2){ // [t1, t2)
		Timestamp ret = null;
		
		if(t1 == null || t2 == null)	return ret;
		
		if(t1.equals(t2))	return ret;
		
		long v1 = t1.getTime();
		long v2 = t2.getTime();
		
		long d1 = v1 % (Interval * 60 * 1000);
		if(d1 == 0)	return t1;
		
		long d2 = d1 + v2 - v1;
		
		if(d2 > (Interval * 60 * 1000)){
			return new Timestamp(v2 - d2 + (Interval * 60 * 1000));
		}
		
		
		return ret;
	}
	
	private Timestamp stdTime = Timestamp.valueOf("2000-01-01 00:00:00");
	private int index(Timestamp t){
		long v = t.getTime();
		v -= stdTime.getTime();
		v = v % (24 * 60 * 60 * 1000);
		v = v / (Interval * 60 * 1000);
		return (int)v;
	}
	private long weight(Timestamp t){ // consistent with index
		long v = t.getTime();
		v -= stdTime.getTime();
		v = v % (24 * 60 * 60 * 1000);
		long d = v / (Interval * 60 * 1000);
		return v - d * (Interval * 60 * 1000);
	}
	
	private double average(double v1, double v2, long w1, long w2){
		return (w2 * v1 + w1 * v2) / (w1 + w2);
	}
	
	private void put(Sketch ske1, Sketch ske2, Timestamp t0){
		Timestamp t1 = ske1.getTime();
		Timestamp t2 = ske2.getTime();
		
		long w1 = t0.getTime() - t1.getTime();
		long w2 = t2.getTime() - t0.getTime();
		
		
		int t = index(t0);
		
		count[t] ++;
		
		{
			Pair pair1 = ske1.zeroOrder.get(t1);
			Pair pair2 = ske2.zeroOrder.get(t2);
			zeroOrderArray[t].v += average(pair1.v, pair2.v, w1, w2);
			zeroOrderArray[t].a += average(pair1.a, pair2.a, w1, w2);
		}
		
		
		for(int h = 0; h < H; h ++){
			for(int n = 0; n < N; n ++){
				Pair pair1 = ske1.firstOrder[h][n].get(t1);
				Pair pair2 = ske2.firstOrder[h][n].get(t2);
				firstOrderArray[t][h][n].v += average(pair1.v, pair2.v, w1, w2);
				firstOrderArray[t][h][n].a += average(pair1.a, pair2.a, w1, w2);
			}
		}
		
		
		for(int h = 0; h < H; h ++){
			for(int n = 0; n < N; n ++){
				for(int n_ = 0; n_ < N; n_ ++){
					Pair pair1 = ske1.secondOrder[h][n][n_].get(t1);
					Pair pair2 = ske2.secondOrder[h][n][n_].get(t2);					
					secondOrderArray[t][h][n][n_].v += average(pair1.v, pair2.v, w1, w2); 
					secondOrderArray[t][h][n][n_].a += average(pair1.a, pair2.a, w1, w2); 
				}
			}
		}
		
		
	}
	
	public Pair zeroOrderDiff(Sketch sketch){
		Timestamp t = sketch.getTime();
		int index1 = index(t);
		long w1 = weight(t);
		int index2 = (index1 + 1) % T;
		long w2 = (Interval * 60 * 1000) - w1;	
		
		
		Pair pair = sketch.zeroOrder.get(t);
		double v = pair.v - average(zeroOrderArray[index1].v / count[index1], zeroOrderArray[index2].v / count[index2], w1, w2);
		double a = pair.a - average(zeroOrderArray[index1].a / count[index1], zeroOrderArray[index2].a / count[index2], w1, w2);
		return new Pair(v, a);
	}
	
	public Pair[][] firstOrderDiff(Sketch sketch){
		Timestamp t = sketch.getTime();
		int index1 = index(t);
		long w1 = weight(t);
		int index2 = (index1 + 1) % T;
		long w2 = (Interval * 60 * 1000) - w1;	
		
		Pair[][] ret = new Pair[H][N];
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				Pair pair = sketch.firstOrder[h][i].get(t);
				double v = pair.v - average(firstOrderArray[index1][h][i].v / count[index1], firstOrderArray[index2][h][i].v / count[index2], w1, w2);
				double a = pair.a - average(firstOrderArray[index1][h][i].a / count[index1], firstOrderArray[index2][h][i].a / count[index2], w1, w2);
				ret[h][i] = new Pair(v, a);
			}
		}
		
		return ret;
	}
	
	public Pair[][][] secondOrderDiff(Sketch sketch){
		Timestamp t = sketch.getTime();
		int index1 = index(t);
		long w1 = weight(t);
		int index2 = (index1 + 1) % T;
		long w2 = (Interval * 60 * 1000) - w1;	
		
		Pair[][][] ret = new Pair[H][N][N];
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				for(int j = 0; j < N; j ++){
					Pair pair = sketch.secondOrder[h][i][j].get(t);
					double v = pair.v - average(secondOrderArray[index1][h][i][j].v / count[index1], secondOrderArray[index2][h][i][j].v / count[index2], w1, w2);
					double a = pair.a - average(secondOrderArray[index1][h][i][j].a / count[index1], secondOrderArray[index2][h][i][j].a / count[index2], w1, w2);
					ret[h][i][j] = new Pair(v, a);
				}	
			}
		}
		
		return ret;
	}
	
	public void print(){
		System.out.println("History--------------------------------------");
		System.out.println("v");
		for(int t = 0; t < T; t ++){
			System.out.println(zeroOrderArray[t].v / count[t]);
		}
		System.out.println("a");
		for(int t = 0; t < T; t ++){
			System.out.println(zeroOrderArray[t].a / count[t]);
		}
		System.out.println("---------------------------------------------");
	}
	

}
