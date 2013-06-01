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

import linegroup3.common.Config;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

import aek.hbasepoller.hbase.Tweet;

//fetch tweets from mysql database 
public class FetcherMS implements FetchTweets, ReadTweets{ 

	private Timestamp start = Config.FetcherMS_start;
	private final Timestamp end = Config.FetcherMS_end;
	
	
	@Override
	public List<JSONObject> fetch() {
		Timestamp next = new Timestamp(start.getTime() + 60 * 1000);
		List<JSONObject> ret = new LinkedList<JSONObject>();
		if(next.after(end)) return ret;
		Statement stmt = null;
		ResultSet rs = null;
		FilterTweet filter = new FilterPopUsers();
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from " + Config.FetcherMS_table + " where t >= \'" + start + "\' and t < \'" + next +"\'" + " order by t";
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					Timestamp t = rs.getTimestamp("t");
					String tweet = rs.getString("tweet");
					if(filter.filterOut(tweet)) {
						//System.out.println(tweet);
						continue;
					}
					
					
					JSONObject obj = new JSONObject();
					obj.put("content", tweet);
					
					SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			        Date date=new Date(t.getTime());
			        String s=sdf.format(date);
					obj.put("publishedTimeGmtStr", s);
					obj.put("geo", rs.getString("geo"));
					
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
	
	
	@Override
	public List<JSONObject> read(Timestamp ts, int minutes) {
		Timestamp start = new Timestamp(ts.getTime() - minutes * 60 * 1000);
		Timestamp end = new Timestamp(ts.getTime());
		
		List<JSONObject> ret = new LinkedList<JSONObject>();
		Statement stmt = null;
		ResultSet rs = null;
		FilterTweet filter = new FilterPopUsers();
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from " + Config.FetcherMS_table + " where t >= \'" + start + "\' and t < \'" + end +"\'" + " order by t";
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					
					String content = rs.getString("tweet");	
					
					if(filter.filterOut(content)) {
						continue;
					}
					
					long statusId = rs.getLong("status_ID");
					long userId = rs.getLong("user_ID");
					Timestamp t = rs.getTimestamp("t");
					String geo = rs.getString("geo");
					
					
					Tweet tObj = new Tweet(statusId, userId, t.getTime(), content, geo);
					
					ret.add(new JSONObject(new Gson().toJson(tObj)));
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
		return ret;
	}
	
	static private Connection conn = null;
	
	static {
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://10.4.8.16/" + Config.FetcherMS_db + "?"
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
