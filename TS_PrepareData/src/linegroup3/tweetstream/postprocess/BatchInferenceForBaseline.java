package linegroup3.tweetstream.postprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;


import linegroup3.common.Config;
import linegroup3.tweetstream.event.Burst;
import linegroup3.tweetstream.event.BurstCompare;
import linegroup3.tweetstream.event.DBAgent;
import linegroup3.tweetstream.event.OnlineEvent;
import linegroup3.tweetstream.preparedata.HashFamily;

public class BatchInferenceForBaseline {
	
	private double[][] e = null; // H*N
	private double Lambda = 0;

	private double[][] x = null; // guess for topics H*N

	

	private final int N = Config.N;
	private final int H = 5;
	
	
	private Set<String> actives = new TreeSet<String>();
	
	List<OnlineEvent> events = new LinkedList<OnlineEvent>();
	
	public void batchInfer(String dirpath) throws Exception{
		File dir = new File(dirpath);
		String[] sketchDirStrs = dir.list();
		for (String sketchDirStr : sketchDirStrs) {	
			
			if(sketchDirStr.startsWith("."))	continue;
			
			File sketchDir = new File(dirpath + sketchDirStr);
			if (sketchDir.isDirectory()) {
				////////// debug ////////
				//if(!sketchDirStr.contains("2011_12_15_03")) continue;
				////////////////////////
				infer(sketchDir.getAbsolutePath());
			}
		}
		
		
		System.out.println("-------------------------------------------------------------------");
		for(OnlineEvent event : events){
			System.out.println(event.toString());
			DBAgent.save_baseline(event);
		}
		
	}
	
	private void infer(String path){	

		
		
		loadV2(path);
		//load(path, 'A');
		initial();		
						
		long ct = System.currentTimeMillis();

		for (int h = 0; h < H; h++) {
			searchTopic(h);
		}

		ct = (System.currentTimeMillis() - ct);
		
		
		//if((fs1.F/fs0.F) <= 1)
		if(!TopicFilter.filterByOptimization(0)){

			
			String[] res = path.split("\\\\");
			String str = res[res.length - 1];
			
			String[] res2 = str.split("_");
			String dateStr = res2[0] + "-" + res2[1] + "-" + res2[2] + " " + res2[3] + ":" + res2[4] + ":" + res2[5];
			
			System.out.println(dateStr + "\t" + ct/1000.0 + "s\t" + (0));
		
			
			List<Burst> bursts = new LinkedList<Burst>();
			analyse(Timestamp.valueOf(dateStr), (0), bursts);
			
			BurstCompare.join(events, bursts);
		}
		else{
			//System.out.println("error!");
		}
		
		//System.out.println();
		//System.out.println("ANALYSING..........................");
		
		
	}
	
	private double searchTopic(int h) {
		
		double s = sum(e[h]);
		
		for(int i = 0; i < N; i ++){
			x[h][i] = e[h][i] /s ;
		}
		
		return 0.0;
	}
	
	
	private void analyse(Timestamp t, double optima, List<Burst> bursts){
		final int TopN = 15;

		PriorityQueue<ValueTermPair> queue = new PriorityQueue<ValueTermPair>(
				TopN, new Comparator<ValueTermPair>() {

					@Override
					public int compare(ValueTermPair arg0, ValueTermPair arg1) {
						if (arg0.v > arg1.v)
							return 1;
						if (arg0.v < arg1.v)
							return -1;
						return 0;
					}

				});

		for (String term : actives) {
			double min = 1e10;
			for (int h = 0; h < H; h++) {
				double v = x[h][HashFamily.hash(h, term)];
				if (v < 0)
					v = 0;
				if (v < min)
					min = v;
			}

			if (min < TopicFilter.minProb)
				continue;

			if (queue.size() < TopN) {
				queue.offer(new ValueTermPair(min, term));
			} else {
				ValueTermPair pair = queue.peek();
				if (pair.v < min) {
					queue.poll();
					queue.offer(new ValueTermPair(min, term));
				}
			}
		}

		if (TopicFilter.filterByTopics(queue)) {
			return;
		}

		Burst burst = new Burst(t, optima);
		for (ValueTermPair pair : queue) {
			System.out.println(pair.term + "\t" + pair.v);
			burst.prob(pair.term, pair.v);
		}
		System.out.println("------------------------------------------");
		bursts.add(burst);
		
	}
	
	private void initial(){
		x = new double[H][N];
	}
	

	
	private double sum(double[] v){
		double ret = 0;
		int n = v.length;
		for(int i = 0; i < n; i ++){
			ret += v[i];
		}
		return ret;
	}
	

	
	private void load(String dir, char c){
		
		String zeroOrder = "diff_zeroOrder";
		String firstOrder = "diff_firstOrder";
		switch (c) {
		case 'V': {
			firstOrder += "V_";
		}
			break;
		case 'A': {
			firstOrder += "A_";
		}
			break;
		}
		
		
		try {
			/////// zeroOrder
			BufferedReader in = new BufferedReader(new FileReader(dir + "/" + zeroOrder + ".txt"));
			String line = null;
			while((line = in.readLine()) != null){
				if(line.startsWith("" + c)){
					String[] res = line.split("\t");
					Lambda = Double.parseDouble(res[1]);
				}
			}
			in.close();
			
			/////// firstOrder
			e = new double[H][N];
			for(int h = 0; h < H; h ++){
				in = new BufferedReader(new FileReader(dir + "/" + firstOrder + h + ".txt"));
				line = in.readLine();
				String[] res = line.split("\t");
				for(int i = 0; i < N; i ++){
					e[h][i] = Double.parseDouble(res[i + 1]);
				}
				in.close();
			}
			
			
			////// for actives
			actives.clear();
			in = new BufferedReader(new FileReader(dir + "/" + "actives.txt"));
			while((line = in.readLine()) != null){
				actives.add(line);
			}
			in.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private void loadV2(String dir){
		Lambda = 0;
		e = new double[H][N];
		loadForV2(dir, 'A');
		loadForV2(dir, 'V');
		
		////// for actives
		actives.clear();
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(dir + "/" + "actives.txt"));
			String line = null;
			while ((line = in.readLine()) != null) {
				actives.add(line);
			}
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private void loadForV2(String dir, char c){
		
		String zeroOrder = "diff_zeroOrder";
		String firstOrder = "diff_firstOrder";
		switch (c) {
		case 'V': {
			firstOrder += "V_";
		}
			break;
		case 'A': {
			firstOrder += "A_";
		}
			break;
		}
		
		
		try {
			/////// zeroOrder
			BufferedReader in = new BufferedReader(new FileReader(dir + "/" + zeroOrder + ".txt"));
			String line = null;
			while((line = in.readLine()) != null){
				if(line.startsWith("" + c)){
					String[] res = line.split("\t");
					Lambda += Double.parseDouble(res[1]);
				}
			}
			in.close();
			
			/////// firstOrder
			for(int h = 0; h < H; h ++){
				in = new BufferedReader(new FileReader(dir + "/" + firstOrder + h + ".txt"));
				line = in.readLine();
				String[] res = line.split("\t");
				for(int i = 0; i < N; i ++){
					e[h][i] += Double.parseDouble(res[i + 1]);
				}
				in.close();
			}
			

			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}
