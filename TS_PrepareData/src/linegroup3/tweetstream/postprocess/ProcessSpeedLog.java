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
		
}
