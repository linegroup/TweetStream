package linegroup3.tweetstream.onlinelda;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import linegroup3.tweetstream.rt2.StopWords;

import org.json.JSONArray;
import org.json.JSONException;


public class RTProcess {
	private static int nTopic = -1;
	private static int gap = -1;
	private static ThreeSigmaMonitor[] monitors = null;
	
	private static List<OnlineEvent> events = new LinkedList<OnlineEvent>();

	//private static Random rand = new Random(); // for debug
	
	public static void runTime(Timestamp start, Timestamp end, Timestamp dt,
			int g, int nT) throws IOException {
		nTopic = nT;
		gap = g;
		OnlineLDA model = new OnlineLDA(50.0 / nTopic, 0.01, nTopic, 100000);
		
		System.out.println("start\t" + start);
		System.out.println("end\t" + end);
		System.out.println("dt\t" + dt);
		System.out.println("gap\t" + gap / (60 * 1000));
		
		int cycle = (24 * 60 * 60 * 1000) / gap;
		monitors = new ThreeSigmaMonitor[nTopic];
		for(int i = 0; i < nTopic; i ++){
			monitors[i] = new ThreeSigmaMonitor(cycle, cycle);
		}
		
		StopWords.initialize();
		System.out.println("-----------------------------------------------------------------------");
		System.out.println();
		
		
		Timestamp next = new Timestamp(start.getTime() + gap);


		while (start.before(end)) {
			System.out.println(new Timestamp(System.currentTimeMillis())
					+ "\tProcessing : " + start); // print info

			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select *  from tokenizedstream where t >= \'"
						+ start + "\' and t < \'" + next + "\' order by t";
				if (stmt.execute(sqlTxt)) {
					
					model.beforeLoad();
					
					rs = stmt.getResultSet();
					while (rs.next()) {
						//if(rand.nextInt(20) != 0) continue; // for debug
						
						String tweet = rs.getString("tweet");
						
						List<String> terms = new LinkedList<String>();
						
						
						try {
							JSONArray array = new JSONArray(tweet);
							for (int k = 0; k < array.length(); k++) {
								terms.add(array.getString(k));
							}
						} catch (org.json.JSONException je) {
							je.printStackTrace();
							continue;
						}

						
						List<String> finalTerms = new LinkedList<String>();
						for(String term : terms){
							if(!StopWords.isStopWord(term)){
								finalTerms.add(term);
							}		
						}
						
						if(finalTerms.size() < 2) continue;
						
						model.loadDoc(finalTerms);
						
					}
					
					long delay = System.currentTimeMillis();
					double[] tracks = model.train(500);
					delay = System.currentTimeMillis() - delay;
					Timestamp detectionTime = new Timestamp(next.getTime() + delay);
					
					for(int i = 0; i < nTopic; i ++){
						if(monitors[i].add(tracks[i])){
							Map<String, Double> topic = model.getTopic(i);
							Burst burst = new Burst(i, detectionTime, start, next, topic);
							add(burst);
						}
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			start = next;
			next = new Timestamp(start.getTime() + gap);
		}
		
		
		resetConnection();
		for(OnlineEvent event : events){
			save(event);
		}

	}
	
	static private void add(Burst burst){
		boolean added = false;
		for(OnlineEvent event : events){
			if(event.getTopicId() == burst.getTopicId()){
				if(burst.getEndTime().getTime() - event.getEnd().getTime() <= 60 * 60 * 1000){  // one hour
					event.add(burst);
					added = true;
					break;
				}
			}
		}
		if(!added){
			OnlineEvent newEvent = new OnlineEvent(burst);
			events.add(newEvent);
		}
	}
	
	static private void save(OnlineEvent event){
		String sqlStr = "insert into onlineldaevents (topicId, start_t, end_t, keywords, detail) values(?, ?, ?, ?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setInt(1, event.getTopicId());
			stmt.setTimestamp(2, event.getStart());
			stmt.setTimestamp(3, event.getEnd());
			stmt.setString(4, event.getKeywordsStr());
			stmt.setString(5, event.getDetail().toString());
			
			stmt.execute();
		} catch(JSONException je){
			je.printStackTrace();
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
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				} // ignore
				stmt = null;
			}
		}
	}

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

}
