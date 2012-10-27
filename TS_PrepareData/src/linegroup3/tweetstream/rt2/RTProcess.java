package linegroup3.tweetstream.rt2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import linegroup3.tweetstream.preparedata.HashFamily;
import linegroup3.tweetstream.rt2.sket.Estimator;
import linegroup3.tweetstream.rt2.sket.OutputSketch;
import linegroup3.tweetstream.rt2.sket.Pair;
import linegroup3.tweetstream.rt2.sket.Sketch;


public class RTProcess {
	static final long oneDayLong = 24 * 60 * 60 * 1000; // (ms)
	
	/////// SET H & K HERE !!!
	static final int H = 5;
	static final int N = 200;

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
		
	
	private static void resetConnection(){
		try {
			conn.close();
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

			conn = null;

		}
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
	
	private static final Timestamp DETECT_T = Timestamp.valueOf("2011-09-04 23:59:59");
	private static final double THRESHOLD_D_V = 1.0;
	private static final double THRESHOLD_D_A = 2.0;
	
	
	private static final int THREAD_POOL_SIZE = 2 * H;
	private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);	
	
	private int LAG = 5; // Largest lag :  5 minutes
	private int CYCLE = 1*60;
	private int MAX_QUEUE_SIZE = CYCLE + LAG; // unit: minute (one day)
	private Sketch[] sketchQueue = new Sketch[MAX_QUEUE_SIZE];
	private int head = 0;
	private int tail = 0;
	
	private ActiveTerm activeTerms = new ActiveTerm();
	
	public RTProcess(){		
		for(int i = 0; i < MAX_QUEUE_SIZE; i ++){
			sketchQueue[i] = new Sketch();
		}
	}
	
	private Sketch currentSketch = null;
	
	public void runTime(Timestamp start, Timestamp end) throws IOException{
		BufferedWriter speedLog = new BufferedWriter(new FileWriter("./data/speedLog.txt"));
		
		Timestamp next = new Timestamp(start.getTime()+oneDayLong);
				
		currentSketch = new Sketch();		

		while(start.before(end)){
			System.out.println("Processing : " + start);  // print info
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select *  from idstream where t >= \'" + start + "\' and t < \'" + next +"\'";
				if (stmt.execute(sqlTxt)) {
					
					rs = stmt.getResultSet();
					while (rs.next()) {
						/// debug
						///long debug_T = System.currentTimeMillis();
						/////////////////////////////////
						
						final Semaphore taskFinished = new Semaphore(1 - 2 * H);
						
						final Timestamp t = rs.getTimestamp("t");
						String tweet = rs.getString("tweet");
						
						////// counting
						double l = 0; 
						ArrayList<TreeMap<Integer, Integer>> counter = new ArrayList<TreeMap<Integer, Integer>>(H);
						for(int h = 0; h < H; h ++){
							counter.add(new TreeMap<Integer, Integer>());
						}
						String[] res = tweet.split(",");
						for(String term : res){
							if(term.length() >= 1){
								int id = Integer.parseInt(term);
								
								activeTerms.active(id, t);
								
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
						
						if(l <= 1){/////////////////////////////// for debug
							System.out.print("L i s less than 2!!!!!!!");
							System.out.println(rs.getString("status_ID"));
							continue;
						}
						
						////////////////////////////////////////////////
						
						double ds = 0;
						////// for zero order
						
						ds = 1;
						currentSketch.zeroOrderPulse(t, ds);


						////// for first order
						for (int h = 0; h < H; h++) {
							final ArrayList<TreeMap<Integer, Integer>> f_counter = counter;
							final int f_h = h;
							final double f_l = l;
							
							pool.execute(new Runnable(){

								@Override
								public void run() {
									for (Map.Entry<Integer, Integer> entry : f_counter
											.get(f_h).entrySet()) {
										int bucket = entry.getKey();
										int count = entry.getValue();
										
										double ds = count / f_l;
										
										currentSketch.firstOrderPulse(t, ds, f_h, bucket);								
									}
									
									taskFinished.release();
								}
								
							});
							/*
							for (Map.Entry<Integer, Integer> entry : counter
									.get(h).entrySet()) {
								int bucket = entry.getKey();
								int count = entry.getValue();

								ds = count / l;
								
								currentSketch.firstOrderPulse(t, ds, h, bucket);								
							}
							*/
						}
					
						
						////// for second order
						for (int h = 0; h < H; h++) {
							final ArrayList<TreeMap<Integer, Integer>> f_counter = counter;
							final int f_h = h;
							final double f_l = l;
							
							pool.execute(new Runnable(){

								@Override
								public void run() {
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
								}
								
							});
							
							/*
							for (Map.Entry<Integer, Integer> entry_i : counter.get(h).entrySet()) {
								int bucket_i = entry_i.getKey();
								int count_i = entry_i.getValue();
								
								for(Map.Entry<Integer, Integer> entry_j : counter.get(h).entrySet()) {
									int bucket_j = entry_j.getKey();
									int count_j = entry_j.getValue();
																		
									if(bucket_i == bucket_j){
										ds = count_i*(count_i-1);		
									}else{
										ds = count_i*count_j;
									}
									ds /= l*(l-1);
									
									currentSketch.secondOrderPulse(t,ds, h, bucket_i, bucket_j);
									
								}
								
							}
							*/
						}	
						
						/////////// synchronize
						try {
							taskFinished.acquire();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
						////////////////// change observing time //////////////
						currentSketch.observe(t);
						
						/////// for difference
						{
							Timestamp oneday_before_t = new Timestamp(t.getTime() - CYCLE * 60 * 1000);
							
							int index = head;
							Sketch sketch2 = sketchQueue[index % MAX_QUEUE_SIZE];
							while(sketch2.getTime().before(oneday_before_t)){
								index ++;
								sketch2 = sketchQueue[index % MAX_QUEUE_SIZE];
							}
							
							Sketch sketch1 = sketchQueue[(index - 1) % MAX_QUEUE_SIZE];
							
							Estimator estimator;
							try {
								estimator = new Estimator(sketch1, sketch2, oneday_before_t);
								
								Pair pair = estimator.zeroOrderDiff(currentSketch);
								
								speedLog.write("" + t + "\t" + pair.v + "\t" + pair.a + "\n");
								
								if(t.after(DETECT_T) && pair.a >= THRESHOLD_D_A && pair.v >= THRESHOLD_D_V){
									saveSketch(currentSketch);
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
							
						}
						
						
						/////// cache snapshot
						final long oneMinute = 60 * 1000;
						
						Timestamp lastone = null;
						if(head == tail){
							currentSketch.copy(sketchQueue[tail]);
							tail ++;
						}else{
							int index = (tail - 1) % MAX_QUEUE_SIZE;
							Sketch lastSketch = sketchQueue[index];
							lastone = lastSketch.getTime();
							if(t.getTime() == lastone.getTime()){
								currentSketch.copy(sketchQueue[index]);
							}
							if(t.getTime() - lastone.getTime() >= oneMinute){
								if(tail - head == MAX_QUEUE_SIZE){
									head ++;
								}
								currentSketch.copy(sketchQueue[tail % MAX_QUEUE_SIZE]);
								tail ++;
							}
						}
						
						/// debug
						///System.out.println(System.currentTimeMillis() - debug_T);
						///////////
						
					}
	
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
			
			
			clearSketchCheckpoints();
			
			start = next;
			next = new Timestamp(start.getTime()+oneDayLong);
			
			resetConnection();
		}
		
		speedLog.close();
	}
	
	private void clearSketchCheckpoints(){
		Sketch sketch = sketchQueue[head % MAX_QUEUE_SIZE];
		Sketch.clearCheckpoints(sketch.getTime());
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
				if(Math.abs(C_V - pair0.v) > 1e-10) System.out.println("V Error!" + h + " " + (C_V - pair0.v) + " " + pair0.v);
				if(Math.abs(C_A - pair0.a) > 1e-10) System.out.println("A Error!" + h + " " + (C_A - pair0.a) + " " + pair0.a);
			}	
		}
		

	}
	
	private void saveSketch(Sketch sketch) throws Exception{
		if(sketch == null)
			sketch = sketchQueue[(tail-1) % MAX_QUEUE_SIZE];
		Timestamp currentTime = sketch.getTime();
		String dir = currentTime.toString();
		dir = dir.replace(" ", "_").replace("-", "_").replace(":", "_").replace(".", "_");
		dir = "./data/sketch/" + dir;
		new File(dir).mkdir();
		
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(dir + "/zeroOrder.txt"));
			out.write(OutputSketch.outputZeroOrder(sketch));
			out.close();
			
			String[] firstOrderA = OutputSketch.outputFirstOrderA(sketch);
			String[] firstOrderV = OutputSketch.outputFirstOrderV(sketch);
			String[] secondOrderA = OutputSketch.outputSecondOrderA(sketch);
			String[] secondOrderV = OutputSketch.outputSecondOrderV(sketch);
			for(int h = 0; h < H; h ++){
				out = new BufferedWriter(new FileWriter(dir + "/firstOrderA_" + h + ".txt"));
				out.write(firstOrderA[h]);
				out.close();
				
				out = new BufferedWriter(new FileWriter(dir + "/firstOrderV_" + h + ".txt"));
				out.write(firstOrderV[h]);
				out.close();
				
				out = new BufferedWriter(new FileWriter(dir + "/secondOrderA_" + h + ".txt"));
				out.write(secondOrderA[h]);
				out.close();
				
				out = new BufferedWriter(new FileWriter(dir + "/secondOrderV_" + h + ".txt"));
				out.write(secondOrderV[h]);
				out.close();
				
					
			}
			
			trackFirstOrder(dir);
			saveActiveTerms(dir);
			
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
		saveSketchDiff(dir, sketch);
		
	}
	
	
	private void saveSketchDiff(String dir, Sketch sketch) throws Exception{
		Timestamp t = sketch.getTime();
		t = new Timestamp(t.getTime() - CYCLE * 60 * 1000);
		
		int index = head;
		Sketch sketch2 = sketchQueue[index % MAX_QUEUE_SIZE];
		while(sketch2.getTime().before(t)){
			index ++;
			sketch2 = sketchQueue[index % MAX_QUEUE_SIZE];
		}
		
		Sketch sketch1 = sketchQueue[(index - 1) % MAX_QUEUE_SIZE];
		
		Estimator estimator = new Estimator(sketch1, sketch2, t);
		
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(dir + "/diff_zeroOrder.txt"));
			out.write(OutputSketch.outputZeroOrder(estimator.zeroOrderDiff(sketch)));
			out.close();
			
			String[] firstOrderA = OutputSketch.outputFirstOrderA(estimator.firstOrderDiff(sketch));
			String[] firstOrderV = OutputSketch.outputFirstOrderV(estimator.firstOrderDiff(sketch));
			String[] secondOrderA = OutputSketch.outputSecondOrderA(estimator.secondOrderDiff(sketch));
			String[] secondOrderV = OutputSketch.outputSecondOrderV(estimator.secondOrderDiff(sketch));
			for(int h = 0; h < H; h ++){
				out = new BufferedWriter(new FileWriter(dir + "/diff_firstOrderA_" + h + ".txt"));
				out.write(firstOrderA[h]);
				out.close();
				
				out = new BufferedWriter(new FileWriter(dir + "/diff_firstOrderV_" + h + ".txt"));
				out.write(firstOrderV[h]);
				out.close();
				
				out = new BufferedWriter(new FileWriter(dir + "/diff_secondOrderA_" + h + ".txt"));
				out.write(secondOrderA[h]);
				out.close();
				
				out = new BufferedWriter(new FileWriter(dir + "/diff_secondOrderV_" + h + ".txt"));
				out.write(secondOrderV[h]);
				out.close();
				
					
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	private void saveActiveTerms(String dir){
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(dir + "/actives.txt"));
			Set<Integer> terms = activeTerms.activeTerms();
			for(int term : terms){
				out.write(term + "\n");
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
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
		
	}
	
}
