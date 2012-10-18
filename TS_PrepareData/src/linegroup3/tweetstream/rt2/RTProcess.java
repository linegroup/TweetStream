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

import linegroup3.tweetstream.preparedata.HashFamily;


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
		
	
	private int LAG = 5; // Largest lag : one 5 minutes
	private int MAX_QUEUE_SIZE = 1*60 + LAG; // unit: minute (one day)
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
	
	public void runTime(Timestamp start, Timestamp end){
		
		Timestamp next = new Timestamp(start.getTime()+oneDayLong);
		
		
		currentSketch = new Sketch();
		Sketch dSketch = new Sketch();
		

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
							for (Map.Entry<Integer, Integer> entry : counter
									.get(h).entrySet()) {
								int bucket = entry.getKey();
								int count = entry.getValue();

								ds = count / l;
								
								currentSketch.firstOrderPulse(t, ds, h, bucket);								
							}
						}
					
						
						////// for second order
						for (int h = 0; h < H; h++) {
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
						}	
						
						
						/////// for difference
						
						////////////////// change observing time //////////////
						currentSketch.observe(t);
						
						
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
			
			start = next;
			next = new Timestamp(start.getTime()+oneDayLong);
		}
	}
	
	private void checkFirstOrder(){
		//Sketch sketch = sketchQueue[(tail-1) % MAX_QUEUE_SIZE];
		Sketch sketch = currentSketch;
		Timestamp currentTime = sketch.getTime();
		System.out.println("Checking..." + currentTime);
		
		{
			Sketch.Pair pair0 = sketch.zeroOrder.get(currentTime);
			for(int h = 0; h < H; h ++){
				double C_V = 0;
				double C_A = 0;
				for(int i = 0; i < N; i ++){
					Sketch.Pair pair1 = sketch.firstOrder[h][i].get(currentTime);
					C_V += pair1.v;
					C_A += pair1.a;
				}
				if(Math.abs(C_V - pair0.v) > 1e-10) System.out.println("V Error!" + h + " " + (C_V - pair0.v) + " " + pair0.v);
				if(Math.abs(C_A - pair0.a) > 1e-10) System.out.println("A Error!" + h + " " + (C_A - pair0.a) + " " + pair0.a);
			}	
		}
		

	}
	
	private void saveSketch(Sketch sketch){
		if(sketch == null)
			sketch = sketchQueue[(tail-1) % MAX_QUEUE_SIZE];
		Timestamp currentTime = sketch.getTime();
		String dir = currentTime.toString();
		dir = dir.replace(" ", "_").replace("-", "_").replace(":", "_").replace(".", "_");
		dir = "./data/sketch/" + dir;
		new File(dir).mkdir();
		
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(dir + "/zeroOrder.txt"));
			out.write(sketch.outputZeroOrder());
			out.close();
			
			String[] firstOrderA = sketch.outputFirstOrderA();
			String[] firstOrderV = sketch.outputFirstOrderV();
			String[] secondOrderA = sketch.outputSecondOrderA();
			String[] secondOrderV = sketch.outputSecondOrderV();
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
			
			saveActiveTerms(dir);
			
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
}
