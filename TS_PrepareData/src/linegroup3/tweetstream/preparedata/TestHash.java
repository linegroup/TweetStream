package linegroup3.tweetstream.preparedata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class TestHash {

	static private Connection conn = null;

	/*
	 * static { try { conn = DriverManager
	 * .getConnection("jdbc:mysql://10.4.8.16/tweetstream?" +
	 * "user=root&password=123583");
	 * 
	 * } catch (SQLException ex) { // handle any errors
	 * System.out.println("SQLException: " + ex.getMessage());
	 * System.out.println("SQLState: " + ex.getSQLState());
	 * System.out.println("VendorError: " + ex.getErrorCode());
	 * 
	 * conn = null;
	 * 
	 * } }
	 */

	static public void doJob() {
		GeneralHashFunctionLibrary hash = new GeneralHashFunctionLibrary();
		for (int i = 0; i < 10000; i++) {
			long hashValue = hash.BKDRHash("" + i) % 100;
			saveHash(i, (int) hashValue);
		}
	}

	static public void test1(int id) {
		try {

			System.out.print(id + ":\t");
			

			int[] v = new int[5];
			for (int i = 0; i < 5; i++) {
				v[i] = HashFamily.hash(i, id);
			}

			List<Integer> list = new LinkedList<Integer>();
			for (int num = 1; num < 10000000; num++) {
				boolean output = true;
				for (int i = 0; i < 5; i++) {
					if (HashFamily.hash(i, num) != v[i]) {
						output = false;
						break;
					}
				}
				if (output) {
					System.out.print(num + ",");
					
					list.add(num);
				}
			}
			System.out.println();


			
			if(list.size() > 1){
				BufferedWriter out = new BufferedWriter(new FileWriter(
						"./hash.text", true));
				out.write(id + ":\t");
				for(int i : list){
					out.write(i + ",");
				}
				out.write("\n");
				out.close();
			}
			
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	static private void saveHash(int id, int hashValue) {
		String sqlStr = "insert into hash (id, hashValue) values(?, ?)";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sqlStr);

			stmt.setInt(1, id);
			stmt.setInt(2, hashValue);

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
