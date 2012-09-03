package linegroup3.tweetstream.preparedata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.TreeSet;





public class LoadTweetsInFile {
	
	static private Connection conn = null;
	
	static private Set<Long> users = new TreeSet<Long>();
	
	static {
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://10.0.106.64/sg_political_tweet?"
							+ "user=xiewei&password=xiewei123456");

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

			conn = null;
		}
	}
	
	static public void GetUsersInfo(){ 
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("./data/2011_09_users.csv"));
			
			out.write("ct,user_ID\n");
			
			stmt = conn.createStatement();

			if (stmt.execute("select  count(*) as ct, user_ID from tweet_2011_09 group by user_ID ")) {
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
}
