package linegroup3.tweetstream.preparedata.othermodel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.TreeMap;

import linegroup3.tweetstream.rt2.StopWords;

public class BuildInput {
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
	
	static TreeMap<Long, BufferedWriter> fileMap = new TreeMap<Long, BufferedWriter>();
	
	public static void build() throws Exception{
		Timestamp startDay = Timestamp.valueOf("2011-09-01 00:00:00.0");

		Timestamp nextDay = new Timestamp(startDay.getTime() + oneDayLong);

		final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");

		while (startDay.before(endDay)) {
			System.out.println(startDay); // print info

			Statement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.createStatement();
				String sqlTxt = "select *  from idstream where t >= \'"
						+ startDay + "\' and t < \'" + nextDay + "\'";
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						long user_ID = Long.parseLong(rs.getString("user_ID"));
						Timestamp t = rs.getTimestamp("t");		
						String tweet = rs.getString("tweet");
						
						BufferedWriter bw = null;
						if(fileMap.containsKey(user_ID)){
							bw = fileMap.get(user_ID);
						}{
							bw = new BufferedWriter(new FileWriter("./data/othermodel/input/" + user_ID + ".txt"));
							fileMap.put(user_ID, bw);
						}
						
						StringBuilder sb = new StringBuilder("" + t + ":");
						String[] res = tweet.split(",");
						for(String word : res){
							if(word.length() >= 1){
								if(!StopWords.isStopWord(Integer.parseInt(word))){
									sb.append(word + " ");
								}
							}
						}
						
						sb.delete(sb.length() - 1, sb.length());
						sb.append("\n");
						
						String str = sb.toString();
						bw.write(str);
						
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

			startDay = nextDay;
			nextDay = new Timestamp(startDay.getTime() + oneDayLong);
		}

		for(BufferedWriter bw : fileMap.values()){
			bw.close();
		}
		
		BufferedWriter filelist = new BufferedWriter(new FileWriter("./data/othermodel/filelist.txt"));
		for(Long user_ID : fileMap.keySet()){
			filelist.write(user_ID + "\n");
		}
		filelist.close();
	}

}
