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
		
		Timestamp start_t = new Timestamp(0);
		Timestamp end_t = new Timestamp(0);
		boolean state = false;
		
		BufferedReader reader = new BufferedReader(new FileReader(dir + "/dspeedlog.txt"));
		String line = null;
		while((line = reader.readLine()) != null){
			String[] res = line.split("\t");
			Timestamp t = Timestamp.valueOf(res[0]);
			Pair pair = new Pair(Double.parseDouble(res[1]), Double.parseDouble(res[2]));
			
			double v = pair.v;
			double a = pair.a;
			
			n += 1;
			s += a;
			s_Sq += a*a;
			
			double e = s/n;
			double e_Sq = s_Sq/n;
			double sigma = Math.sqrt(e_Sq - e*e);
			
			if(state == false){
				if((a-e > 3*sigma) && (v > 1)){
					state = true;
					start_t = t;
				}
			}else{
				end_t = t;
				if(!((a-e > 3*sigma) && (v > 1))){
					if(end_t.getTime() - start_t.getTime() >= 15*60*1000){
						state = false;
						System.out.println(3*sigma + "\t" + e + "\t[" + start_t + "\t" + end_t + "]");
					} 
				}
			}

		}
		reader.close();
	}

}
