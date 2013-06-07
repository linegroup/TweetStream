package linegroup3.experiment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import linegroup3.common.Config;
import linegroup3.tweetstream.event.Burst;


public class ExperimentAgent {
	
	static private Random rand = new Random();

	static public long getId(){
		return Math.abs(rand.nextLong());
	}
	
	static public void createTable(long testId){
		String sqlStr = "create table test_" + testId + " ( t Timestamp, bursts text, primary key(t)) engine myisam";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);
			
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
	
	static private JSONArray detailOfBurst(List<Burst> bursts){
		JSONArray ret = null;
		
		try {
			ret = new JSONArray();
			for (Burst b : bursts) {
				JSONObject burst = new JSONObject();
				JSONObject obj = new JSONObject(b.getDistribution());
				
				burst.put("p", obj);
				burst.put("t", b.getTime().toString());
				burst.put("op", b.getOptima());

				ret.put(burst);
			}

		} catch (org.json.JSONException e) {
			e.printStackTrace();
		}
		
		return ret;
	}
	
	static public void saveInfo(long testId, Timestamp t, List<Burst> bursts){
		JSONArray array = detailOfBurst(bursts);
		if(array == null)	return;
		
		String text = array.toString();
		
		String sqlStr = "insert into test_" + testId +  " (t, bursts) values(?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, t);

			stmt.setString(2, text);
			
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

	static public void saveParameters(long testId, String parameters){
		String sqlStr = "insert into parameters (test_id, parameters) values(?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setString(1, "test_" + testId);

			stmt.setString(2, parameters);
			
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
	
	static public void scanBursts(long testId){
		Timestamp start = Config.historyS;
		Timestamp end = Config.historyE;
		Timestamp next = new Timestamp(start.getTime() + 60 * 60 * 1000);

		while (true) {
			if (next.after(end))
				break;
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select *  from " + "test_" + testId
						+ " where t >= \'" + start + "\' and t < \'" + next
						+ "\'" + " order by t";
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						Timestamp t = rs.getTimestamp("t");
						String text = rs.getString("bursts");

						List<Burst> bursts = new LinkedList<Burst>();
						JSONArray array = new JSONArray(text);
						for(int i = 0; i < array.length(); i ++){
							bursts.add(new Burst(array.getJSONObject(i)));
						}
						
						GenerateEventsFromBursts.process(t, bursts);
					}
				}
			} catch (SQLException ex) {
				// handle any errors
				System.out.println("SQLException: " + ex.getMessage());
				System.out.println("SQLState: " + ex.getSQLState());
				System.out.println("VendorError: " + ex.getErrorCode());

			} catch (JSONException e) {
				e.printStackTrace();
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
			start = next;
			next = new Timestamp(start.getTime() + 60 * 60 * 1000);
		}
		
		GenerateEventsFromBursts.endProcess();
	}
	
	static public void main(String[] args){
		scanBursts(1539744993195673470L);
	}
	
	
	static private Connection conn = null;
	
	static {
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://10.4.8.16/" + "experiment" + "?"
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
