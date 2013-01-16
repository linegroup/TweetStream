package linegroup3.tweetstream.postprocess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

public class Statistics {
	
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
	
	public static void job1() throws Exception{
		BufferedReader in = new BufferedReader(new FileReader("C:\\Users\\wei.xie.2012\\Desktop\\jobs.txt"));
		
		String line = null;
		
		while((line = in.readLine()) != null){
			String[] res = line.split("\t");
			
			String[] res2 = res[0].split(" ");
			
			String str = res2[1];
			
			str = str.substring(0, 5);
			
			
			String sqlStr = "insert into temp  values(?) ";
			PreparedStatement stmt = null;
			try {
				stmt = conn.prepareStatement(sqlStr);
				stmt.setString(1, str);
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
		
		
		in.close();
		
		
	}
	
	public static void trackWord(){
		// parameters :
		
		String word = "smrt";
		String database = "stream";
		Timestamp start = Timestamp.valueOf("2011-12-14 00:00:00");
		Timestamp end = Timestamp.valueOf("2011-12-17 00:00:00");
		
		///////////////////////////////////////////
		
		String sqlTxt = "select t from " + database + " where tweet like \"" + word + "\" and t >= " + start + " and t < " + end + " order by t";
		
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			if (stmt.execute(sqlTxt)) {
				
				rs = stmt.getResultSet();
				while (rs.next()) {
					Timestamp t = rs.getTimestamp("t");
					
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

}
