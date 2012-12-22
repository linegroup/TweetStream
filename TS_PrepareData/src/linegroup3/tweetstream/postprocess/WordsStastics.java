package linegroup3.tweetstream.postprocess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;

import cmu.arktweetnlp.Twokenize;

import linegroup3.tweetstream.preparedata.HashFamily;
import linegroup3.tweetstream.preparedata.Tweet;
import linegroup3.tweetstream.rt2.StopWords;
import linegroup3.tweetstream.rt2.sket.Estimator;
import linegroup3.tweetstream.rt2.sket.Pair;
import linegroup3.tweetstream.rt2.sket.Sketch;

public class WordsStastics {
	static final long oneDayLong = 24 * 60 * 60 * 1000; // (ms)
	
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
	
	private static String decode(String tweet){
		return org.apache.commons.lang.StringEscapeUtils.unescapeHtml(tweet);
	}
	
	private static String downcase(String tweet) {
		return tweet.toLowerCase();
	}
	
	private static List<String> tokenize(String tweet) {
		String str = tweet;
		str = str.replaceAll("\\.{10,}+", " ");
		List<String> terms = Twokenize.tokenize(str);

		/*
		final String regex = "\\p{Punct}+";
		for (String term : terms) {
			if (term.length() > 0 && term.length() <= 64
					&& !term.matches(regex)) {
				ret.add(term);
			}
		}
		*/

		return terms;

	}
	
	public static void runTime(Timestamp start, Timestamp end) throws IOException{	
		
		Set<String> words = new TreeSet<String>();
		Map<Integer, Integer> len1Counter = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> len2Counter = new TreeMap<Integer, Integer>();
		
		StopWords.initialize();

		Timestamp next = new Timestamp(start.getTime()+oneDayLong);
				
		while(start.before(end)){
			System.out.println(new Timestamp(System.currentTimeMillis()) + "\tProcessing : " + start);  // print info
			
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				String sqlTxt = "select *  from stream where t >= \'" + start + "\' and t < \'" + next +"\'";
				if (stmt.execute(sqlTxt)) {
					
					rs = stmt.getResultSet();
					while (rs.next()) {
												
						String tweet = rs.getString("tweet");
						
						tweet = decode(tweet);
						tweet = downcase(tweet);
						List<String> terms = tokenize(tweet);
						
						List<String> terms_after_remove_stopwords = new LinkedList<String>();
						
						for(String term : terms){
							if(!StopWords.isStopWord(term)){
								terms_after_remove_stopwords.add(term);
								
								words.add(term);
							}
						}
						
						if(terms.size() > 140){
							System.out.println("----------------------------------------------------------");
							System.out.println(tweet);
							System.out.println("----------------------------------------------------------");
						}
						
						if(len1Counter.containsKey(terms.size())){
							len1Counter.put(terms.size(), len1Counter.get(terms.size()) + 1);
						}else{
							len1Counter.put(terms.size(), 1);
						}
						
						
						if(len2Counter.containsKey(terms_after_remove_stopwords.size())){
							len2Counter.put(terms_after_remove_stopwords.size(), len2Counter.get(terms_after_remove_stopwords.size()) + 1);
						}else{
							len2Counter.put(terms_after_remove_stopwords.size(), 1);
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
						
			start = next;
			next = new Timestamp(start.getTime()+oneDayLong);
			
		}
		
		System.out.println("size of words : " + words.size());
		
		resetConnection();
		
		for(Map.Entry<Integer, Integer> entry : len1Counter.entrySet()){
			saveLen1(entry.getKey(), entry.getValue());
		}
		
		for(Map.Entry<Integer, Integer> entry : len2Counter.entrySet()){
			saveLen2(entry.getKey(), entry.getValue());
		}
		
	}
	
	
	private static void saveLen1(int len, int cnt){
		String sqlStr = "insert into tweetlen1 (len, cnt) values(?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setInt(1, len);
			stmt.setInt(2, cnt);
			
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
	
	private static void saveLen2(int len, int cnt){
		String sqlStr = "insert into tweetlen2 (len, cnt) values(?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setInt(1, len);
			stmt.setInt(2, cnt);
			
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
	
	private static void resetConnection(){
		try {
			conn.close();
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

			conn = null;

		}
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
