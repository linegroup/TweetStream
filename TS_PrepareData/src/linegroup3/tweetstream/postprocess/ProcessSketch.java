package linegroup3.tweetstream.postprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;

public class ProcessSketch {
	
	public static void processFirstOrder(String dir) throws Exception {
		File path = new File(dir);
		File[] files = path.listFiles();
		for(File file : files){
			if(!file.isDirectory()) continue;
			
			String prefix = file.getPath();
			
			LinkedList<Double> list = new LinkedList<Double>();
			for(int i = 0; i < 5; i ++){
				String name = prefix + "\\diff_firstOrderA_" + i + ".txt";
				BufferedReader reader = new BufferedReader(new FileReader(name));
				String line = reader.readLine();
				reader.close();
				
				double max = 0;
				String[] res = line.split("\t");
				for(String term : res){
					if(term.length() > 0){
						double value= Double.parseDouble(term);
						if(value > max){
							max = value;
						}
					}
				}
				
				list.add(max);
			}
			
			double max = 0;
			for(double value : list){
				if(value > max){
					max = value;
				}
			}
			if(max < 10) continue;
			list.add(max);
			
			StringBuilder sb = new StringBuilder();
			for(double value : list){
				sb.append("\t" + value);
			}
			System.out.println(file.getName() + sb.toString());
		}
	}

}
