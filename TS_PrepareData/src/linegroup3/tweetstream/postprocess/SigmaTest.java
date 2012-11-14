package linegroup3.tweetstream.postprocess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Timestamp;

import linegroup3.tweetstream.rt2.sket.Pair;

public class SigmaTest {
		
	public static void test(String dir) throws Exception {
		int n = 0;
		double s = 0;
		double s_Sq = 0;
		
		BufferedReader reader = new BufferedReader(new FileReader(dir + "/dspeedlog.txt"));
		String line = null;
		while((line = reader.readLine()) != null){
			String[] res = line.split("\t");
			Pair pair = new Pair(Double.parseDouble(res[1]), Double.parseDouble(res[2]));
			
			double v = pair.v;
			double a = pair.a;
			
			n += 1;
			s += a;
			s_Sq += a*a;
			
			double e = s/n;
			double e_Sq = s_Sq/n;
			double sigma = Math.sqrt(e_Sq - e*e);
			
			if(a-e > 3*sigma && v > 1){
				System.out.println(line);
			}
		}
		reader.close();
	}

}
