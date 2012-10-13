package linegroup3.tweetstream.rt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import linegroup3.tweetstream.preparedata.HashFamily;



public class Process {
	static final long oneDayLong = 24 * 60 * 60 * 1000; // (ms)
	
	static final long smoothLength1 = 15; // minute
	static final long smoothLength2 = 5; // minute
	static final long oneMinute = 60 * 1000; // (ms)
	

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
	
	
	private int H = 0;
	private int N = 0;
	
	
	private TreeMap<Timestamp, SVA_Sketch> sketchQueue = new TreeMap<Timestamp, SVA_Sketch>();
	private int MAX_QUEUE_SIZE = 24*60 + 1; // unit: minute (one day)
	
	public Process(int H, int N){
		this.H = H;
		this.N = N;
	}
	
	public void runTime(Timestamp start, Timestamp end){
		
		Timestamp next = new Timestamp(start.getTime()+oneDayLong);
		
		
		long smooth1 = smoothLength1 * oneMinute ; // ms
		long smooth2 = smoothLength2 * oneMinute ; // ms
		
		SVA_Sketch currentSketch = new SVA_Sketch(H, N);
		SVA_Sketch dSketch = new SVA_Sketch(H, N);
		

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
						
						currentSketch.observe(t);
						
						double ds = 0;
						
						////// for zero order
						
						ds = 1;
						currentSketch.s.zeroOrder.pulse(t, ds, 0);
						double dv = currentSketch.v.zeroOrder.pulse(t, ds/smoothLength1, smooth1);
						currentSketch.a.zeroOrder.pulse(t, dv/smoothLength2, smooth2);

						////// counting
						double l = 0; 
						ArrayList<TreeMap<Integer, Integer>> counter = new ArrayList<TreeMap<Integer, Integer>>(H);
						String[] res = tweet.split(",");
						for(String term : res){
							if(term.length() > 1){
								int id = Integer.parseInt(term);
								
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

						////// for first order
						for (int h = 0; h < H; h++) {
							for (Map.Entry<Integer, Integer> entry : counter
									.get(h).entrySet()) {
								int bucket = entry.getKey();
								int count = entry.getValue();

								ds = count / l;

								currentSketch.s.firstOrder[h][bucket].pulse(t,ds, 0);
								dv = currentSketch.v.firstOrder[h][bucket].pulse(t, ds/smoothLength1, smooth1);
								currentSketch.a.firstOrder[h][bucket].pulse(t, dv/smoothLength2, smooth2);
								
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
									
									currentSketch.s.secondOrder[h][bucket_i][bucket_j].pulse(t,ds, 0);
									dv = currentSketch.v.secondOrder[h][bucket_i][bucket_j].pulse(t, ds/smoothLength1, smooth1);
									currentSketch.a.secondOrder[h][bucket_i][bucket_j].pulse(t, dv/smoothLength2, smooth2);
								}
								
							}
						}	
						
						
						/////// for difference
						
						
						/////// cache snapshot
						final long oneMinute = 60 * 1000;
						
						Timestamp lastone = sketchQueue.lowerKey(t);
						if(lastone == null){
							sketchQueue.put(t, currentSketch.copy());
						}else{
							if(t.getTime() - lastone.getTime() >= oneMinute){
								if(sketchQueue.size() >= MAX_QUEUE_SIZE){
									sketchQueue.pollFirstEntry();
								}
								sketchQueue.put(t, currentSketch.copy());
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

}
