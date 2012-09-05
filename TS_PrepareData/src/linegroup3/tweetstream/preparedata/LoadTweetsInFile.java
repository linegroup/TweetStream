package linegroup3.tweetstream.preparedata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;






public class LoadTweetsInFile {
	
	static private Connection conn = null;
	static private Connection conn2 = null;
	
	static private Map<Long, Tweet> tweets = new TreeMap<Long, Tweet>();
	
	static {
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://10.0.106.64/sg_political_tweet?"
							+ "user=xiewei&password=xiewei123456");
			
			conn2 = DriverManager
					.getConnection("jdbc:mysql://10.4.8.16/tweetstream?"
							+ "user=root&password=123583");

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

			conn = null;
			conn2 = null;
		}
	}
	
	static public void loadTweets(){
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			
			stmt = conn.createStatement();

			if (stmt.execute("select published_time_GMT, status_ID, user_ID, content from tweet_2011_10")) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					
					Timestamp time = rs.getTimestamp("published_time_GMT");
					String status_ID = rs.getString("status_ID");
					String user_ID = rs.getString("user_ID");
					String twt = rs.getString("content");
					
					save(new Tweet(user_ID,  status_ID,  twt,  time));
					
				}
			}
			
			//out.close();

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
	}
	
	static public void statistic(){
		Statement stmt = null;
		ResultSet rs = null;
		
		

		for(int date = 1; date <= 30; date ++)
		try {
			String time = "2011-10-";
			
			if(date < 10){
				time += "0" + date;
			}else{
				time += date;
			}
			
			
			String sqlTxt = "select count(*) as cnt from tweet_2011_10 where published_time_GMT >= \'" + time + " 00:00:00\'" + 
					" and published_time_GMT <= \'" + time + " 23:59:59\'";
			
			stmt = conn.createStatement();

			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					int cnt = rs.getInt("cnt");
					System.out.print(cnt + "\t");
				}
			}
			
			sqlTxt += " and content like \'%jobs%\'";
			
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					int cnt = rs.getInt("cnt");
					System.out.println(cnt);
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
	}
	
	static public void statistic2(){
		Statement stmt = null;
		ResultSet rs = null;
		
		

		for(int hour = 0; hour < 24; hour ++)
		try {
			String time = "2011-10-07";
			
			String hourTxt = hour + "";
			if(hour < 10){
				hourTxt = "0" + hour;
			}
			
			System.out.print(hour + "\t");
			
			String sqlTxt = "select count(*) as cnt from tweet_2011_10 where published_time_GMT >= \'" + time + " " + hourTxt + ":00:00\'" + 
					" and published_time_GMT <= \'" + time + " " + hourTxt + ":59:59\'";
			
			stmt = conn.createStatement();

			sqlTxt += " and content like \'%jobs%\'";
			
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					int cnt = rs.getInt("cnt");
					System.out.println(cnt);
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
	}
	
	
	static public void statistic3(){
		Statement stmt = null;
		ResultSet rs = null;
		
		
		for(int hour = 15; hour <24 ; hour ++)
		for(int min = 0; min < 60; min ++)
		try {
			String time = "2011-10-05";
			
			String hourTxt = "" + hour;
			
			String minTxt = "" + min;
			if(min <10)	minTxt = "0" + minTxt;
			
			String sqlTxt = "select count(*) as cnt from tweet_2011_10 where published_time_GMT >= \'" + time + " " + hourTxt + ":" + minTxt + ":00\'" + 
					" and published_time_GMT <= \'" + time + " " + hourTxt + ":" + minTxt + ":59\'";
			
			stmt = conn.createStatement();

			//sqlTxt += " and content like \'%jobs%\'";
			//sqlTxt += " and content like \'% 56%\'";
			sqlTxt += " and content like \'%steve%\'";
			
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					int cnt = rs.getInt("cnt");
					System.out.println(cnt);
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
	}
	
	static public void getUsersInfo(){ 
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("./data/2011_12_users.csv"));
			
			out.write("ct,user_ID\n");
			
			stmt = conn.createStatement();

			if (stmt.execute("select  count(*) as ct, user_ID from tweet_2011_12 group by user_ID ")) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					int ct = rs.getInt("ct");
					String user_ID = rs.getString("user_ID");
					
					out.write("" + ct + "," + user_ID + "\n");
				}
			}
			
			out.close();

		} catch (IOException ioe){
			ioe.printStackTrace();
			
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
	}
	
	private static void save(Tweet tweet){
		String sqlStr = "insert into stream (t, status_ID, user_ID, tweet) values(?, ?, ?, ?) ";
		PreparedStatement stmt = null;
		try {
			stmt = conn2.prepareStatement(sqlStr);

			stmt.setTimestamp(1, tweet.t);
			stmt.setString(2, tweet.status_ID);
			stmt.setString(3, tweet.user_ID);
			stmt.setString(3, tweet.twt);
			
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
