package linegroup3.tweetstream.preparedata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import cmu.arktweetnlp.Twokenize;

public class Preprocess {
	// downcasing
	// remove rt
	// words counting

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

	private static Map<String, Integer> counter = new TreeMap<String, Integer>();

	public static void doCounting() {
		Timestamp startDay = Timestamp.valueOf("2011-01-01 00:00:00.0");

		Timestamp nextDay = new Timestamp(startDay.getTime() + oneDayLong);

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
						count(tweet);
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

		System.out.println("Size of the counter" + counter.size());
		for (Map.Entry<String, Integer> entry : counter.entrySet()) {
			saveCount(entry.getKey(), entry.getValue());
		}
	}

	public static void constructStream3() {
		Set<String> users = new TreeSet<String>();

		Statement stmt = null;
		ResultSet rs = null;
		try {

			stmt = conn.createStatement();
			String sqlTxt = "select *  from users";
			if (stmt.execute(sqlTxt)) {
				rs = stmt.getResultSet();
				while (rs.next()) {
					users.add(rs.getString("user_ID"));
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

		Timestamp startDay = Timestamp.valueOf("2011-09-01 00:00:00.0");

		Timestamp nextDay = new Timestamp(startDay.getTime() + oneDayLong);

		final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");

		while (startDay.before(endDay)) {
			System.out.println(startDay); // print info

			stmt = null;
			rs = null;
			try {

				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream where t >= \'"
						+ startDay + "\' and t < \'" + nextDay + "\'";
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						String user_ID = rs.getString("user_ID");
						Tweet tweet = new Tweet(user_ID,
								rs.getString("status_ID"),
								rs.getString("tweet"), rs.getTimestamp("t"));
						if (users.contains(user_ID)) {
							save(tweet);
						}
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

	}

	public static void doJob() {
		Timestamp startDay = Timestamp.valueOf("2011-01-01 00:00:00.0");

		Timestamp nextDay = new Timestamp(startDay.getTime() + oneDayLong);

		final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");

		while (startDay.before(endDay)) {
			System.out.println(startDay); // print info

			Statement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream where t >= \'"
						+ startDay + "\' and t < \'" + nextDay + "\'";
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						Tweet tweet = new Tweet(rs.getString("user_ID"),
								rs.getString("status_ID"),
								rs.getString("tweet"), rs.getTimestamp("t"));
						downcase(tweet);
						removeRT(tweet);
						count(tweet);
						save(tweet);
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

		for (Map.Entry<String, Integer> entry : counter.entrySet()) {
			saveCount(entry.getKey(), entry.getValue());
		}
	}

	private static void downcase(Tweet tweet) {
		tweet.twt = tweet.twt.toLowerCase();
	}

	private static void removeRT(Tweet tweet) {
		tweet.twt = tweet.twt.replaceFirst("^rt @.+:", "");
	}

	private static void count(Tweet tweet) {
		// String[] terms =
		// tweet.twt.split("[\\s!\\(\\)*+,-.:;<=>?\\[\\]^`\\{|\\}~\"]+"); //
		// save # $ % & / \ @ ' _
		String str = tweet.twt;
		str = str.replaceAll("\\.{10,}+", " ");
		List<String> terms = Twokenize.tokenize(str);

		final String regex = "\\p{Punct}+";
		Set<String> set = new TreeSet<String>();
		for (String term : terms) {
			if (term.length() > 0 && term.length() <= 64
					&& !term.matches(regex)) {
				set.add(term);
			}
		}

		for (String term : set) {
			Integer cnt = counter.get(term);
			if (cnt == null) {
				counter.put(term, 1);
			} else {
				counter.put(term, cnt + 1);
			}
		}
	}

	private static List<String> tokenize(String tweet) {
		List<String> ret = new LinkedList<String>();
		String str = tweet;
		str = str.replaceAll("\\.{10,}+", " ");
		List<String> terms = Twokenize.tokenize(str);

		final String regex = "\\p{Punct}+";
		for (String term : terms) {
			if (term.length() > 0 && term.length() <= 64
					&& !term.matches(regex)) {
				ret.add(term);
			}
		}

		return ret;

	}

	private static void save(Tweet tweet) {
		String sqlStr = "insert into idstream (t, status_ID, user_ID, tweet) values(?, ?, ?, ?) ";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setTimestamp(1, tweet.t);
			stmt.setString(2, tweet.status_ID);
			stmt.setString(3, tweet.user_ID);
			stmt.setString(4, tweet.twt);

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

	private static void saveCount(String term, int count) {
		String sqlStr = "insert into termcount (word, count) values(?, ?) "; // !!!!!
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setString(1, term);
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

	public static void buildTermIndex() {
		Map<Integer, String> idTermIndex = new TreeMap<Integer, String>();
		Map<String, Integer> termIdIndex = new TreeMap<String, Integer>();

		int id = 0;

		Timestamp startDay = Timestamp.valueOf("2011-09-01 00:00:00.0");

		Timestamp nextDay = new Timestamp(startDay.getTime() + oneDayLong);

		final Timestamp endDay = Timestamp.valueOf("2012-05-01 00:00:00.0");

		int onelentwt = 0;
		while (startDay.before(endDay)) {
			System.out.println(startDay); // print info

			Statement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream3 where t >= \'"
						+ startDay + "\' and t < \'" + nextDay + "\'";
				if (stmt.execute(sqlTxt)) {
					rs = stmt.getResultSet();
					while (rs.next()) {
						Tweet tweet = new Tweet(rs.getString("user_ID"),
								rs.getString("status_ID"),
								rs.getString("tweet"), rs.getTimestamp("t"));
						downcase(tweet);
						removeRT(tweet);
						List<String> list = tokenize(tweet.twt);
						if(list.size() <= 1) {onelentwt ++ ; continue;}
						StringBuilder IdTwt = new StringBuilder();
						for (String term : list) {
							Integer termId = termIdIndex.get(term);
							if (termId == null) {
								id++;
								termId = id;
								idTermIndex.put(id, term);
								termIdIndex.put(term, id);
								IdTwt.append(termId + ",");
								
								saveTermId(term, termId);
							} else {
								IdTwt.append(termId + ",");
							}
						}
						tweet.twt = new String(IdTwt);
						save(tweet);
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
		
		System.out.println("onelentwt: " + onelentwt);

	}
	
	private static void saveTermId(String term, int id) {
		String sqlStr = "insert into termid (term, id) values(?, ?) "; // !!!!!
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setString(1, term);
			stmt.setInt(2, id);

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
		
		sqlStr = "insert into idterm (id, term) values(?, ?) "; // !!!!!
		stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setInt(1, id);
			stmt.setString(2, term);

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
