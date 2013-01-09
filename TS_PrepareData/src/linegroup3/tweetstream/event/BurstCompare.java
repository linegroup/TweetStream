package linegroup3.tweetstream.event;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BurstCompare {
	static private final double JOIN_THRESHOLD = 0.0025;
	static public final double MIN_SUPPORT = 0.025;
	static private final long T_GAP = 60 * 60 * 1000; // 1 hour

/*
	static public List<Burst> merge(List<Burst> bursts){
		if(bursts.size() <= 1){
			return bursts;
		}
		
		List<Burst> ret = new LinkedList<Burst>();
		
		for(Burst burst : bursts){
			if(!same(ret, burst)){
				ret.add(burst);
			}
		}
		
		reduceIntersection(ret);
		return ret;
	}
	
	static private boolean same(List<Burst> bursts, Burst newBurst){
		for(Burst burst : bursts){
			if(burst.similarity(newBurst) > SIMILARITY_THRESHOLD){
				return true;
			}
		}
		return false;
	}
	
	static private void reduceIntersection(List<Burst> bursts){
		String[] maxWords = new String[bursts.size()];
		int i = 0;
		for(Burst burst : bursts){
			maxWords[i] = maxProb(burst);
			i ++;
		}
		
		i = 0;
		for(Burst burst : bursts){
			for(int j = 0; j < bursts.size(); j ++){
				if(j != i){
					burst.getDistribution().remove(maxWords[j]);
				}
			}
			i ++;
		}
	}
	
	
	static private String maxProb(Burst burst){
		double max = 0.0;
		String ret = null;
		for(Map.Entry<String, Double> entry : burst.getDistribution().entrySet()){
			double p = entry.getValue();
			if(p > max){
				max = p;
				ret = entry.getKey();
			}
		}
		
		return ret;
	}*/
	
	static private boolean join(OnlineEvent event, Burst burst){
		if(burst.getTime().after(new Timestamp(event.getEnd().getTime() + T_GAP))){
			return false;
		}
		
		double s = 0.0;
		
		for(Map.Entry<String, Double> entry : burst.getDistribution().entrySet()){
			String word = entry.getKey();
			double p = entry.getValue();
			s += p * event.support(word);
		}

		return  s > JOIN_THRESHOLD;
	}
	
	static public void join(List<OnlineEvent> events, List<Burst> bursts){
		int n = bursts.size();
		Burst[] ee = new Burst[n];
		OnlineEvent[] er = new OnlineEvent[n];		
				
		int i = 0; int j = 0;
		for(Burst burst : bursts){
			boolean joined = false;
			ee[i] = burst;
			for(OnlineEvent event : events){
				if(join(event, burst)){

					er[i] = event;
					
					joined = true;
					break;
				}
			}
			if(!joined){
				er[i] = null;
			}
			
			i ++;
		}
		
		/////////////////////// remove intersection words
		for(i = 0; i < n; i ++){
			if(er[i] != null)
			for(j = 0; j < n; j ++){
				if(er[j] != null && er[i] != er[j]){
					for(String word : commonWords(ee[j], er[j])){
						ee[i].getDistribution().remove(word);
					}
				}
			}
		}		
		
		
		for (j = 0; j < n; j++) {
			if (er[j] != null) {
				er[j].add(ee[j]);
			} else {

				OnlineEvent event = new OnlineEvent(ee[j]);
				if (event.getKeywords().size() > 0) {
					events.add(event);
				}

			}
		}
		
	}
	
	static private List<String> commonWords(Burst burst, OnlineEvent event){
		List<String> ret = new LinkedList<String>();
		for(String word : burst.getDistribution().keySet()){
			if(event.getKeywords().contains(word)){
				ret.add(word);
			}
		}	
		return ret;
	}

}
