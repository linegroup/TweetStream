package linegroup3.tweetstream.preparedata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;

public class Speed {

	static final long oneDayLong = 24 * 60 * 60 * 1000; // (ms)
	
	static final long smoothLength1 = 15; // minute
	static final long smoothLength2 = 15; // minute
	static final long oneMinute = 60 * 1000; // (ms)
	

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
		Timestamp startDay = Timestamp.valueOf("2011-10-01 00:00:00.0");
		
		Timestamp nextDay = new Timestamp(startDay.getTime()+oneDayLong);
		
		//final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");
		final Timestamp endDay = Timestamp.valueOf("2011-10-07 00:00:00.0");
		
		
		long smooth1 = smoothLength1 * oneMinute ; // ms
		long smooth2 = smoothLength2 * oneMinute ; // ms
		
		
		Timestamp t0 = Timestamp.valueOf("2000-01-01 00:00:00.0"); // this value doesn't matter
		double s = 0;
		double v = 0;
		double a = 0;
		while(startDay.before(endDay)){
			System.out.println(startDay);  // print info
			
			
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				
				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream3 where t >= \'" + startDay + "\' and t < \'" + nextDay +"\'";
				if (stmt.execute(sqlTxt)) {
					Map<Timestamp, SVA> map = new TreeMap<Timestamp,SVA>();
					rs = stmt.getResultSet();
					while (rs.next()) {
						Timestamp t = rs.getTimestamp("t");
						double ds = 1;
						s += ds;
						double dt = t.getTime()-t0.getTime();
						double e1 = Math.exp(-dt/smooth1);
						double v0 = v;
						v = v*e1 + ds;
						double dv = v - v0;
						double e2 = Math.exp(-dt/smooth2);
						a = a*e2 + dv/smoothLength1;
						
						final long oneMinute = 60 * 1000;
						
						if(t.getTime() % oneMinute == 0)
							map.put(t, new SVA(s, v/smoothLength1, a/smoothLength2));
						
						/*
						if(dt == 0){
							updateSpeed(t, s, v, a);
						}else{
							saveSpeed(t, s, v, a);
						}
						*/
						
						t0 = t;
					}
					for(Map.Entry<Timestamp, SVA> entry : map.entrySet()){
						SVA sva = entry.getValue();
						saveSpeed(entry.getKey(), sva.s, sva.v, sva.a);
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
			nextDay = new Timestamp(startDay.getTime()+oneDayLong);
		}
	}
	
	/*
	private static void updateSpeed(Timestamp t, double s, double v, double a){
		String sqlStr = "update speed set s=?, v=?, a=? where t = ?";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setDouble(1, s);
			stmt.setDouble(2, v);
			stmt.setDouble(3, a);
			stmt.setTimestamp(4, t);
			
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
	*/
	
	private static void saveDSpeed(Timestamp t, double s, double v, double a){
		String sqlStr = "insert into dspeed (t, s, v, a) values(?, ?, ?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, t);
			stmt.setDouble(2, s);
			stmt.setDouble(3, v);
			stmt.setDouble(4, a);
			
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
	
	private static void saveSpeed(Timestamp t, double s, double v, double a){
		String sqlStr = "insert into speed (t, s, v, a) values(?, ?, ?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, t);
			stmt.setDouble(2, s);
			stmt.setDouble(3, v);
			stmt.setDouble(4, a);
			
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
	
	static public void showDSpeed(Timestamp startTime, Timestamp endTime){ //[startTime, endTime)
		
		while(startTime.before(endTime)){
			Timestamp onedaybefore = new Timestamp(startTime.getTime() - oneDayLong);
			SVA sva1 = getSpeed(startTime);
			SVA sva0 = getSpeed(onedaybefore);
			saveDSpeed(startTime, sva1.s - sva0.s, sva1.v - sva0.v, sva1.a - sva0.a);
			
			startTime = new Timestamp(startTime.getTime() + oneMinute);
		}
	}
	
	static public SVA getSpeed(Timestamp time){
		Statement stmt = null;
		ResultSet rs = null;
		
		Timestamp t1 = null;
		SVA sva1 = null;
		
		Timestamp t2 = null;
		SVA sva2 = null;
		try {
			
			stmt = conn.createStatement();
			String sqlTxt = "select *  from speed where t <= \'" + time + "\' order by t desc limit 1";
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					t1 = rs.getTimestamp("t");
					sva1 = new SVA(rs.getDouble("s"), rs.getDouble("v"), rs.getDouble("a"));
					break;
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
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				} // ignore
				stmt = null;
			}
		}
		
		if(time.equals(t1))	return sva1;
		
		try {
			
			stmt = conn.createStatement();
			String sqlTxt = "select *  from speed where t > \'" + time + "\' order by t  limit 1";
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					t2 = rs.getTimestamp("t");
					sva2 = new SVA(rs.getDouble("s"), rs.getDouble("v"), rs.getDouble("a"));
					break;
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
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				} // ignore
				stmt = null;
			}
		}
		
		
		long dt = t2.getTime() - t1.getTime();
		long dt1 = time.getTime() - t1.getTime();
		long dt2 = t2.getTime() - time.getTime();
		
		double w1 = (double)dt1/dt;
		double w2 = (double)dt2/dt;
		
		return new SVA(sva1.s*w2 + sva2.s*w1, sva1.v*w2 + sva2.v*w1, sva1.a*w2 + sva2.a*w1);
		
		
	}


}


