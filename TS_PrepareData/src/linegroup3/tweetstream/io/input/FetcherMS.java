package linegroup3.tweetstream.io.input;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONException;
import org.json.JSONObject;

//fetch tweets from mysql database 
public class FetcherMS implements FetchTweets{ 

	private Timestamp start = Timestamp.valueOf("2013-01-25 00:00:00");
	private final Timestamp end = Timestamp.valueOf("2013-02-18 00:00:00");
	
	
	@Override
	public List<JSONObject> fetch() {
		Timestamp next = new Timestamp(start.getTime() + 60 * 1000);
		if(next.after(end)) return null;
		List<JSONObject> ret = new LinkedList<JSONObject>();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from newstream3 where t >= \'" + start + "\' and t < \'" + next +"\'" + " order by t";
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					Timestamp t = rs.getTimestamp("t");
					String tweet = rs.getString("tweet");
					if(tweet.startsWith("RT @")) continue;
					
					
					JSONObject obj = new JSONObject();
					obj.put("content", tweet);
					
					SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			        Date date=new Date(t.getTime());
			        String s=sdf.format(date);
					obj.put("publishedTimeGmtStr", s);
					
					ret.add(obj);
				}
			}
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

		} catch (JSONException e) {
			e.printStackTrace();
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
		return ret;
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

}
