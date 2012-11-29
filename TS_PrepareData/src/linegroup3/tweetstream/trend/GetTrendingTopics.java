package linegroup3.tweetstream.trend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;



import twitter4j.Trend;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class GetTrendingTopics {

	static private Twitter twitter = new TwitterFactory().getInstance();
	
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
	
	static public void run(){
		long t = Timestamp.valueOf("2012-11-28 16:35:00").getTime();
		long oneHour = 5*60*1000 - 21; // 5 min
		Date date = new Date(t); 
		
		
		Timer timer = new Timer();
		
	
		timer.schedule(new TimerTask(){

			@Override
			public void run() {
				try{
					get();	
				}catch(TwitterException te){
					te.printStackTrace();
				}
			}
			
		}, date, oneHour);
		
		System.out.println("start...");
		
	}

	static private void get() throws TwitterException{
		/*
		ResponseList<Trends> list = twitter.getDailyTrends();
				
		for(Trends trends : list){
			System.out.println("////////////////////////////////////////");
			System.out.println(trends.getAsOf());
			System.out.println(trends.getTrendAt());
			System.out.println(trends.getLocation());
			for(Trend trend : trends.getTrends()){
				System.out.println(trend);
			}
		}
		*/
		/*
		ResponseList<Location> list = twitter.getAvailableTrends();
		
		for(Location location : list){
			System.out.println(location);
		}
		*/
		
		/*
		Trends trends = twitter.getLocationTrends(23424948);
		System.out.println(trends.getAsOf());
		System.out.println(trends.getTrendAt());
		System.out.println(trends.getLocation());
		for(Trend trend : trends.getTrends()){
			System.out.println(trend);
		}*/
		
		//System.out.println(new Timestamp(System.currentTimeMillis()));
		
		System.out.println("get...\n" + (new Timestamp(System.currentTimeMillis())));
		
		resetConnection();
		Trends trends = twitter.getLocationTrends(23424948);
		save(trends);
		
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
	
	private static void save(Trends trends) {
		String sqlStr = "insert into roughTrend2 (t, asof, trendat, location, trends) values(?, ?, ?, ?, ?) ";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
			stmt.setString(2, trends.getAsOf().toString());
			stmt.setString(3, trends.getTrendAt().toString());
			stmt.setString(4, trends.getLocation().toString());
			
			StringBuilder sb = new StringBuilder();
			for(Trend trend : trends.getTrends()){
				sb.append(trend.toString() + "\n");
			}
			
			stmt.setString(5, sb.toString());

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
