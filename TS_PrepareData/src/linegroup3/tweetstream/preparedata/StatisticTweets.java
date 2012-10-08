package linegroup3.tweetstream.preparedata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

public class StatisticTweets {

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
	
	static public void roughAnalysisPerWeek(){
		try{
			BufferedReader in = new BufferedReader(new FileReader("./data/stat_cnt_per_day.csv"));
			BufferedWriter out = new BufferedWriter(new FileWriter("./data/stat_tweets_cnt_per_week.csv"));
			
			out.write("Time,Sat,Sun,Mon,Tue,Wed,Thu,Fri\n");
			
			in.readLine();
			String line = null;
			
			int[] week = new int[7];
			int i = 0;
			while((line = in.readLine()) != null){
				String[] res = line.split(",");
				String time = res[0];
				int cnt = Integer.parseInt(res[1]);
				week[i]=cnt;
				i = (i+1)%7;
				
				if(i == 0){
					out.write(time + ",");
					for(int j = 0; j < 7; j ++){
						if(j != 6)
							out.write(week[j] + ",");
						else
							out.write(week[j] + "\n");
					}
				}
			}
			out.close();
			in.close();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	static public void roughAnalysisPerWeek2(){
		try{
			BufferedReader in = new BufferedReader(new FileReader("./data/stat_cnt_per_day.csv"));
			BufferedWriter out = new BufferedWriter(new FileWriter("./data/stat_users_cnt_per_week.csv"));
			
			out.write("Time,Sat,Sun,Mon,Tue,Wed,Thu,Fri\n");
			
			in.readLine();
			String line = null;
			
			int[] week = new int[7];
			int i = 0;
			while((line = in.readLine()) != null){
				String[] res = line.split(",");
				String time = res[0];
				int cnt = Integer.parseInt(res[2]);
				week[i]=cnt;
				i = (i+1)%7;
				
				if(i == 0){
					out.write(time + ",");
					for(int j = 0; j < 7; j ++){
						if(j != 6)
							out.write(week[j] + ",");
						else
							out.write(week[j] + "\n");
					}
				}
			}
			out.close();
			in.close();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	static public void roughAnalysisPerDay(){
		Timestamp startDay = Timestamp.valueOf("2011-01-01 00:00:00.0");
		
		Timestamp nextDay = new Timestamp(startDay.getTime()+oneDayLong);
		
		final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");
		
		try{
			BufferedWriter out = new BufferedWriter(new FileWriter("./data/stat_cnt_per_day.csv"));
			out.write("Day,TweetCnt,UserCnt\n");
		
		
		while(startDay.before(endDay)){
			String day = startDay.toString().split(" ")[0];
			
			int tweets_cnt = -1;
			int users_cnt = -1;
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				
				stmt = conn.createStatement();
				String sqlTxt = "select count(*) as cnt from stream where t >= \'" + startDay + "\' and t < \'" + nextDay +"\'";
				//System.out.println(sqlTxt);
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						
						tweets_cnt = rs.getInt("cnt");
						
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
			
			try {
				
				stmt = conn.createStatement();
				String sqlTxt = "select count(distinct user_ID) as cnt from stream where t >= \'" + startDay + "\' and t < \'" + nextDay +"\'";
				//System.out.println(sqlTxt);
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						
						users_cnt = rs.getInt("cnt");
						
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
			
			String msg = day + "," + tweets_cnt + "," + users_cnt;
			System.out.println(msg);
			out.write(msg + "\n");
			
			startDay = nextDay;
			nextDay = new Timestamp(startDay.getTime()+oneDayLong);
		}
		
		out.close();
		
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	
}
