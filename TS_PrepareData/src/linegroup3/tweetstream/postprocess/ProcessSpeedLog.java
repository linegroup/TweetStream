package linegroup3.tweetstream.postprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;

import linegroup3.tweetstream.rt2.sket.Pair;

public class ProcessSpeedLog {

	public static void process() throws Exception {
		TreeMap<Timestamp, Pair> map = new TreeMap<Timestamp, Pair>();
		
		BufferedReader reader = new BufferedReader(new FileReader("./data/speedlog.txt"));
		String line = null;
		while((line = reader.readLine()) != null){
			String[] res = line.split("\t");
			Timestamp t = Timestamp.valueOf(res[0]);
			Pair pair = new Pair(Double.parseDouble(res[1]), Double.parseDouble(res[2]));
			map.put(t, pair);
		}		
		
		reader.close();
		
		BufferedWriter writer = new BufferedWriter(new FileWriter("./data/processedspeedlog.txt"));
		for(Map.Entry<Timestamp, Pair> entry : map.entrySet()){
			Timestamp t = entry.getKey();
			Pair pair = entry.getValue();
			
			writer.write("" + t + "\t" + pair.v + "\t" + pair.a + "\n");
		}
		writer.close();
	}
	
	public static void printD(String dir) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(dir + "/dspeedlog.txt"));
		BufferedWriter writer = new BufferedWriter(new FileWriter(dir + "/printDspeedlog.txt"));
		String line = null;
		final int COUNT = 1000;
		int cnt = 0;
		while((line = reader.readLine()) != null && cnt < COUNT){
			writer.write(line);
			writer.write("\n");
			
			cnt ++;
		}
		writer.close();
		reader.close();	
	}
	
	public static void processD(String dir) throws Exception {
		Timestamp t0 = new Timestamp(0);
		String line0 = null;
		BufferedReader reader = new BufferedReader(new FileReader(dir + "/dspeedlog.txt"));
		BufferedWriter writer = new BufferedWriter(new FileWriter(dir + "/processedDspeedlog.txt"));
		String line = null;
		//final int COUNT = 1000;
		//int cnt = 0;
		while((line = reader.readLine()) != null /*&& cnt < COUNT*/){
			String[] res = line.split("\t");
			Timestamp t = Timestamp.valueOf(res[0]);
			if(!t0.equals(t)){
				if(line0 != null){
					writer.write(line0);
					writer.write("\n");
				}
			}
			t0 = t;
			line0 = line;
			
			//cnt ++;
		}
		writer.write(line0);
		writer.write("\n");
		writer.close();
		reader.close();		
	}
	
	public static void process2(String dir) throws Exception {
		TreeMap<Timestamp, Pair> map = new TreeMap<Timestamp, Pair>();
		
		BufferedReader reader = new BufferedReader(new FileReader(dir + "/speedlog.txt"));
		String line = null;
		final int COUNT = 100000;
		int cnt = 0;
		while((line = reader.readLine()) != null && cnt < COUNT){
			String[] res = line.split("\t");
			Timestamp t = Timestamp.valueOf(res[0]);
			Pair pair = new Pair(Double.parseDouble(res[1]), Double.parseDouble(res[2]));
			map.put(t, pair);
			cnt ++;
		}		
		
		reader.close();
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(dir + "/processedspeedlog.txt"));
		for(Map.Entry<Timestamp, Pair> entry : map.entrySet()){
			Timestamp t = entry.getKey();
			Pair pair = entry.getValue();
			
			writer.write("" + t + "\t" + pair.v + "\t" + pair.a + "\n");
		}
		writer.close();
	}
	
	public static void trimD(String dir) throws Exception{
		Timestamp st = Timestamp.valueOf("2011-10-05 12:00:00"); // excluded
		Timestamp et = Timestamp.valueOf("2011-10-06 00:00:00"); // included
		
		BufferedReader reader = new BufferedReader(new FileReader(dir + "/processedDspeedlog.txt"));
		BufferedWriter writer = new BufferedWriter(new FileWriter(dir + "/trimD.txt"));
		String line = null;
		while((line = reader.readLine()) != null){
			String[] res = line.split("\t");
			Timestamp t = Timestamp.valueOf(res[0]);
			
			if(t.after(et))
				break;
			
			if(t.after(st)){
				writer.write(line);
				writer.write("\n");
			}
			

		}
		writer.close();
		reader.close();
	}
		
}
