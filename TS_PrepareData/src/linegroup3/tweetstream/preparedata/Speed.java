package linegroup3.tweetstream.preparedata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Speed {

	static final long oneDayLong = 24 * 60 * 60 * 1000; // (ms)
	
	static final long smoothLength = 60 * 1000; // (ms)  ----> one minute

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

	public static void doJob() {

	}

}
