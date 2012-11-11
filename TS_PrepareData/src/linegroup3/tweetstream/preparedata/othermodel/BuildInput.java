package linegroup3.tweetstream.preparedata.othermodel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

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
	
	static TreeSet<Long> users = new TreeSet<Long>();

	public static void build() throws Exception {
		StopWords.initialize();
		
		final int RD = 50;
		for (int round = 0; round < RD; round++) {
			System.out.println("Round " + round);
			
			TreeMap<Long, LinkedList<String>> fileMap = new TreeMap<Long, LinkedList<String>>();
			
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
							long user_ID = Long.parseLong(rs
									.getString("user_ID"));
							
							if(user_ID % RD != round) continue;
							
							Timestamp t = rs.getTimestamp("t");
							String tweet = rs.getString("tweet");

							LinkedList<String> bw = null;
							if (fileMap.containsKey(user_ID)) {
								bw = fileMap.get(user_ID);
							}
							else{
								bw = new LinkedList<String>();
								fileMap.put(user_ID, bw);
							}

							StringBuilder sb = new StringBuilder();
							sb.append(t.toString());
							sb.delete(sb.length() - 2, sb.length());
							sb.append(":");
							
							String[] res = tweet.split(",");
							for (String word : res) {
								if (word.length() >= 1) {
									if (!StopWords.isStopWord(Integer
											.parseInt(word))) {
										sb.append(word + " ");
									}
								}
							}

							sb.delete(sb.length() - 1, sb.length());

							String str = sb.toString();
							bw.add(str);

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
			
			for(Map.Entry<Long, LinkedList<String>> entry : fileMap.entrySet()){
				long user_ID = entry.getKey();
				LinkedList<String> bw = entry.getValue();
				
				users.add(user_ID);
				
				BufferedWriter file = new BufferedWriter(new FileWriter(
						"D:/othermodel/input/" + user_ID
						+ ".txt"));
				for(String str : bw){
					file.write(str);
					file.write("\n");
				}
				file.close();
			}

			fileMap.clear();
		}
		
		BufferedWriter filelist = new BufferedWriter(new FileWriter(
				"D:/othermodel/filelist.txt"));
		for (Long user_ID : users) {
			filelist.write(user_ID + "\n");
		}
		filelist.close();
	}

}
