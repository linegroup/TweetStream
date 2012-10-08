package linegroup3.tweetstream.preparedata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Preprocess {
	// downcasing
	// remove rt
	// words counting
	
	
	static final long oneDayLong = 24*60*60*1000; // ms
	
	static private Connection conn = null;
	static{
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
	
	private static Map<String, Integer> counter = new TreeMap<String, Integer>();
	
	public static void doJob(){
		Timestamp startDay = Timestamp.valueOf("2011-01-01 00:00:00.0");
		
		Timestamp nextDay = new Timestamp(startDay.getTime()+oneDayLong);
		
		final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");
		
		while(startDay.before(endDay)){
			System.out.println(startDay);  // print info
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				
				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream where t >= \'" + startDay + "\' and t < \'" + nextDay +"\'";
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						Tweet tweet = new Tweet(rs.getString("user_ID"), rs.getString("status_ID"), rs.getString("tweet"), rs.getTimestamp("t"));
						downcase(tweet);
						removeRT(tweet);
						count(tweet);
						save(tweet);
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
			
			startDay = nextDay;
			nextDay = new Timestamp(startDay.getTime()+oneDayLong);
		}
		
		
		for(Map.Entry<String, Integer> entry : counter.entrySet()){
			saveCount(entry.getKey(),entry.getValue());
		}
	}
	
	private static void downcase(Tweet tweet){
		tweet.twt = tweet.twt.toLowerCase();
	}
	
	private static void removeRT(Tweet tweet){
		tweet.twt = tweet.twt.replaceFirst("^rt @.+:", "");
	}
	
	private static void count(Tweet tweet){
		String[] terms = tweet.twt.split("\\s+");
		
		Set<String> set = new TreeSet<String>();
		for(String term : terms){
			if(term.length() > 0){
				set.add(term);
			}
		}
		
		for(String term : set){
			Integer cnt = counter.get(term);
			if(cnt == null){
				counter.put(term, 1);
			}else{
				counter.put(term, cnt + 1);
			}
		}
	}
	
	
	private static void save(Tweet tweet){
		String sqlStr = "insert into stream2 (t, status_ID, user_ID, tweet) values(?, ?, ?, ?) ";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, tweet.t);
			stmt.setString(2, tweet.status_ID);
			stmt.setString(3, tweet.user_ID);
			stmt.setString(4, tweet.twt);
			
			stmt.execute();
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
	
	private static void saveCount(String term, int count){
		String sqlStr = "insert into wordcount (word, count) values(?, ?) ";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setString(1, term);
			stmt.setInt(2, count);

			
			stmt.execute();
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
	
}
