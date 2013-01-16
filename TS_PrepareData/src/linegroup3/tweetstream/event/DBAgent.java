package linegroup3.tweetstream.event;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import linegroup3.tweetstream.burstdetect.TwoStateMachine;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DBAgent {
	
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
	
	static public void save(OnlineEvent event){
		String sqlStr = "insert into events (start_t, end_t, keywords, detail) values(?, ?, ?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, event.getStart());
			stmt.setTimestamp(2, event.getEnd());
			stmt.setString(3, event.getKeywordsStr());
			stmt.setString(4, event.getDetail().toString());
			
			stmt.execute();
		} catch(JSONException je){
			je.printStackTrace();
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
	
	
	static public void save_baseline(OnlineEvent event){
		String sqlStr = "insert into events_baseline (start_t, end_t, keywords, detail) values(?, ?, ?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, event.getStart());
			stmt.setTimestamp(2, event.getEnd());
			stmt.setString(3, event.getKeywordsStr());
			stmt.setString(4, event.getDetail().toString());
			
			stmt.execute();
		} catch(JSONException je){
			je.printStackTrace();
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
	
	/////// build chart //////////////////////////////////////////////////////////////////////////////
	static public void processEvents(){		
		
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from events";
			if (stmt.execute(sqlTxt)) {
				
				rs = stmt.getResultSet();
				while (rs.next()) {
					int id = rs.getInt("id");
					Timestamp start_t = rs.getTimestamp("start_t");
					String keywords = rs.getString("keywords");
					JSONArray array = new JSONArray(keywords);
					
					Set<String> words = new TreeSet<String>();
					for(int i = 0; i < array.length(); i ++){
						words.add(array.getString(i));
					}
					
					////////////////////////////////////////
					///if(id != 20)	continue;
					////////////////////////////////////////
					
					//JSONArray curve = getEventCurve(start_t, words, 8, 2, 5 *60 * 1000);
					
					//saveCurve(id, start_t, curve.toString(), "eventchart_minute");
					
					JSONArray curve = getEventCurve(start_t, words);
					saveCurve(id, start_t, curve.toString(), "eventchart");
				}
				
			}
		} catch(JSONException je){
			je.printStackTrace();
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
	
	static public void processEvents_baseline(){		
		
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from events_baseline";
			if (stmt.execute(sqlTxt)) {
				
				rs = stmt.getResultSet();
				while (rs.next()) {
					int id = rs.getInt("id");
					Timestamp start_t = rs.getTimestamp("start_t");
					String keywords = rs.getString("keywords");
					JSONArray array = new JSONArray(keywords);
					
					Set<String> words = new TreeSet<String>();
					for(int i = 0; i < array.length(); i ++){
						words.add(array.getString(i));
					}
					
					////////////////////////////////////////
					///if(id != 20)	continue;
					////////////////////////////////////////
					
					//JSONArray curve = getEventCurve(start_t, words, 8, 2, 5 *60 * 1000);
					
					//saveCurve(id, start_t, curve.toString(), "eventchart_minute");
					
					JSONArray curve = getEventCurve(start_t, words);
					saveCurve(id, start_t, curve.toString(), "eventchart_baseline");
				}
				
			}
		} catch(JSONException je){
			je.printStackTrace();
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
	
	static private void saveCurve(int id, Timestamp t, String curve, String db){
		String sqlStr = "insert into " + db + " (id, t, curve) values(?, ?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);
			stmt.setInt(1, id);
			stmt.setTimestamp(2, t);
			stmt.setString(3, curve);
			
			stmt.execute();
		}catch (SQLException ex) {
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
	
	
	static public JSONArray getEventCurve(Timestamp start_t, Set<String> keywords, int gap0, int gap1, int unit){
		JSONArray ret = new JSONArray();
		
		Timestamp start = new Timestamp(start_t.getTime() - gap0 * unit);
		Timestamp end = new Timestamp(start_t.getTime() + gap1 * unit);

		Timestamp next = new Timestamp(start.getTime() + unit); // 1 unit

		while (start.before(end)) {
			int count = 0;
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select tweet  from tokenizedstream where t >= \'" + start
						+ "\' and t < \'" + next + "\'";
				if (stmt.execute(sqlTxt)) {

					rs = stmt.getResultSet();
					while (rs.next()) {
						String tweet = rs.getString("tweet");
						
						JSONArray array = new JSONArray(tweet);
						
						List<String> items = new LinkedList<String>();
						for(int k = 0; k < array.length(); k ++){
							items.add(array.getString(k));
						}
						
						
						int cnt = 0;
						Set<String> itemset = new TreeSet<String>();
						for(String iterm : items){
							itemset.add(iterm);
						}
						
						for(String item : itemset){
							if(keywords.contains(item)){
								cnt ++;
							}
						}
						
						if(cnt >= 2){
							count ++;
						}else{
							if(keywords.size() < 2 && cnt == 1){
								count ++;
							}
						}

					}
					
					JSONObject obj = new JSONObject();
					obj.put("t", start);
					obj.put("v", count);
					ret.put(obj);
					
				}
			} catch(JSONException je){
				je.printStackTrace();
			}
			catch (SQLException ex) {
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
			
			start = next;
			next = new Timestamp(start.getTime() + unit); // 1 unit

		}

		return ret;
	}
	
	
	static public JSONArray getEventCurve(Timestamp start_t, Set<String> keywords){
		JSONArray ret = new JSONArray();
		
		final int GAP = 15; // 15 days
		Timestamp start = new Timestamp(start_t.getTime() - GAP * 24 * 60 * 60 * 1000);
		Timestamp end = new Timestamp(start_t.getTime() + GAP * 24 * 60 * 60 * 1000);

		Timestamp next = new Timestamp(start.getTime() + 24 * 60 * 60 * 1000); // 1 day

		while (start.before(end)) {
			int count = 0;
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select tweet  from tokenizedstream where t >= \'" + start
						+ "\' and t < \'" + next + "\'";
				if (stmt.execute(sqlTxt)) {

					rs = stmt.getResultSet();
					while (rs.next()) {
						String tweet = rs.getString("tweet");
						
						JSONArray array = new JSONArray(tweet);
						
						List<String> items = new LinkedList<String>();
						for(int k = 0; k < array.length(); k ++){
							items.add(array.getString(k));
						}
						
						
						int cnt = 0;
						Set<String> itemset = new TreeSet<String>();
						for(String iterm : items){
							itemset.add(iterm);
						}
						
						for(String item : itemset){
							if(keywords.contains(item)){
								cnt ++;
							}
						}
						
						if(cnt >= 2){
							count ++;
						}else{
							if(keywords.size() < 2 && cnt == 1){
								count ++;
							}
						}

					}
					
					JSONObject obj = new JSONObject();
					obj.put("t", start);
					obj.put("v", count);
					ret.put(obj);
					
				}
			} catch(JSONException je){
				je.printStackTrace();
			}
			catch (SQLException ex) {
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
			
			start = next;
			next = new Timestamp(start.getTime() + 24 * 60 * 60 * 1000); // 1 day

		}

		return ret;
	}
	
	////////////// test 2sm ////////////////////////////////////////////////////////////
	static public void testBurstOnTwoStateMachine() { // test2sm
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from eventchart";
			if (stmt.execute(sqlTxt)) {

				rs = stmt.getResultSet();
				while (rs.next()) {
					int id = rs.getInt("id");
					String curve = rs.getString("curve");
					
					JSONArray array = new JSONArray(curve);
					int n = array.length();
					int[] count = new int[n];
					
					for(int i = 0; i < n; i ++){
						count[i] = array.getJSONObject(i).getInt("v");
					}
					
					int[] state = new TwoStateMachine().load(count).infer();
					
					boolean result = state[15] == 1;
					
					save(id, result);
				}
			}
		} catch (JSONException je) {
			je.printStackTrace();
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
	
	static private void save(int id, boolean result){
		String sqlStr = "insert into test2sm (id, result) values(?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);
			stmt.setInt(1, id);
			stmt.setBoolean(2, result);
			
			stmt.execute();
		}catch (SQLException ex) {
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
	
	////////////// test 2sm baseline////////////////////////////////////////////////////////////
	static public void testBurstOnTwoStateMachine_baseline() { // test2sm baseline
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from eventchart_baseline";
			if (stmt.execute(sqlTxt)) {

				rs = stmt.getResultSet();
				while (rs.next()) {
					int id = rs.getInt("id");
					String curve = rs.getString("curve");
					
					JSONArray array = new JSONArray(curve);
					int n = array.length();
					int[] count = new int[n];
					
					for(int i = 0; i < n; i ++){
						count[i] = array.getJSONObject(i).getInt("v");
					}
					
					int[] state = new TwoStateMachine().load(count).infer();
					
					boolean result = state[15] == 1;
					
					save_baseline(id, result);
				}
			}
		} catch (JSONException je) {
			je.printStackTrace();
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
	
	static private void save_baseline(int id, boolean result){
		String sqlStr = "insert into test2sm_baseline (id, result) values(?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);
			stmt.setInt(1, id);
			stmt.setBoolean(2, result);
			
			stmt.execute();
		}catch (SQLException ex) {
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
	
	
	static public void testBurstOnTwoStateMachine_minute() { // test2sm
		int tcount = 0;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			String sqlTxt = "select *  from eventchart_minute";
			if (stmt.execute(sqlTxt)) {

				rs = stmt.getResultSet();
				while (rs.next()) {
					int id = rs.getInt("id");
					String curve = rs.getString("curve");
					
					JSONArray array = new JSONArray(curve);
					int n = array.length();
					int[] count = new int[n];
					
					for(int i = 0; i < n; i ++){
						count[i] = array.getJSONObject(i).getInt("v");
					}
					
					int[] state = new TwoStateMachine().load(count).infer();
					
					int first = Integer.MIN_VALUE;
					for(int i = 0; i < state.length; i ++){
						if(state[i] == 1){
							first = i;
							break;
						}
					}
					
					
					boolean result = state[8] == 1;
					
					if(result)	tcount ++;
					
					//save(id, result);
					System.out.println("" + id + "\t" + result + "\t" + (first - 8));
				}
			}
		} catch (JSONException je) {
			je.printStackTrace();
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
		
		System.out.println("count:" + tcount);
	}
	
	////////////////////////////////////////////////////////////////////////////
	

}
