package linegroup3.tweetstream.onlinelda;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import linegroup3.tweetstream.rt2.StopWords;

import org.json.JSONArray;


public class RTProcess {

	private static OnlineLDA model = new OnlineLDA(50.0 / 100, 0.01, 100, 100000);

	public static void runTime(Timestamp start, Timestamp end, Timestamp dt,
			long gap) throws IOException {

		System.out.println("start\t" + start);
		System.out.println("end\t" + end);
		System.out.println("dt\t" + dt);
		System.out.println("gap\t" + gap / (60 * 1000));
		
		StopWords.initialize();
		System.out.println("-----------------------------------------------------------------------");
		System.out.println();
		
		
		Timestamp next = new Timestamp(start.getTime() + gap);


		while (start.before(end)) {
			System.out.println(new Timestamp(System.currentTimeMillis())
					+ "\tProcessing : " + start); // print info

			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select *  from tokenizedstream where t >= \'"
						+ start + "\' and t < \'" + next + "\' order by t";
				if (stmt.execute(sqlTxt)) {
					
					model.beforeLoad();
					
					rs = stmt.getResultSet();
					while (rs.next()) {
						String tweet = rs.getString("tweet");
						
						List<String> terms = new LinkedList<String>();
						
						
						try {
							JSONArray array = new JSONArray(tweet);
							for (int k = 0; k < array.length(); k++) {
								terms.add(array.getString(k));
							}
						} catch (org.json.JSONException je) {
							je.printStackTrace();
							continue;
						}

						
						List<String> finalTerms = new LinkedList<String>();
						for(String term : terms){
							if(!StopWords.isStopWord(term)){
								finalTerms.add(term);
							}		
						}
						
						if(finalTerms.size() < 2) continue;
						
						model.loadDoc(finalTerms);
						
					}
					
					model.train(1000);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			start = next;
			next = new Timestamp(start.getTime() + gap);
		}

	}

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

}
