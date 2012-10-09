package linegroup3.tweetstream.preparedata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Set;
import java.util.TreeSet;

public class ConstructUserTable {
	// use the first 8 months in 2011

	static final long oneDayLong = 24 * 60 * 60 * 1000; // ms

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

	static final Set<String> users = new TreeSet<String>();
	
	static public void doJob(){
		load();
	}
	
	static private void save(String user){
		String sqlStr = "insert into allusers (user_ID) values(?) ";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setString(1, user);
		
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

	static private void load() {
		Timestamp startDay = Timestamp.valueOf("2011-01-01 00:00:00.0");

		Timestamp nextDay = new Timestamp(startDay.getTime() + oneDayLong);

		//final Timestamp endDay = Timestamp.valueOf("2011-09-01 00:00:00.0");  // !!!!!!! the first 8 months
		final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");
		
		while (startDay.before(endDay)) {
			System.out.println(startDay); // print info

			Statement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream2 where t >= \'"
						+ startDay + "\' and t < \'" + nextDay + "\'";
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						Tweet tweet = new Tweet(rs.getString("user_ID"),
								rs.getString("status_ID"),
								rs.getString("tweet"), rs.getTimestamp("t"));
						users.add(tweet.user_ID);
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

			int count = users.size();
			saveUsersCount(startDay, count);
			System.out.println("" + startDay + ":\t" + count);
			
			startDay = nextDay;
			nextDay = new Timestamp(startDay.getTime() + oneDayLong);
			
			
		}
		
		System.out.println("Number of users: " + users.size());
		
		
		for(String user : users){
			save(user);
		}
	}
	
	private static void saveUsersCount(Timestamp t, int count){
		String sqlStr = "insert into userscount (t, count) values(?, ?) "; // !!!!!
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, t);
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
