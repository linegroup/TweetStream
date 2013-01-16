package linegroup3.tweetstream.postprocess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import org.json.JSONArray;


public class TokenizeStream {
	
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
	
	public static void tokenize(){
		Timestamp start = Timestamp.valueOf("2010-01-01 00:00:00");
		Timestamp end = Timestamp.valueOf("2013-01-01 00:00:00");
		
		Timestamp next = new Timestamp(start.getTime()+oneDayLong);
		
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
												
						String tweet = rs.getString("tweet");		

						List<String> terms = TokenizeTweet.tokenizeTweet(tweet);
												
						JSONArray array = new JSONArray(terms);
						
						Timestamp t = rs.getTimestamp("t");
						
						String status_ID = rs.getString("status_ID");
						String user_ID = rs.getString("user_ID");
						
						
						save(t, status_ID, user_ID, array.toString());
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
	
	private static void save(Timestamp t, String status_ID, String user_ID, String tweet){
		String sqlStr = "insert into tokenizedstream (t, status_ID, user_ID, tweet) values(?, ?, ?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, t);
			stmt.setString(2, status_ID);
			stmt.setString(3, user_ID);
			stmt.setString(4, tweet);
			
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
