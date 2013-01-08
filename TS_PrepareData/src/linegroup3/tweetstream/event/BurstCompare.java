package linegroup3.tweetstream.event;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BurstCompare {
	static private final double JOIN_THRESHOLD = 0.05;
	static private final double SIMILARITY_THRESHOLD = 0.01;
	static public final double MIN_SUPPORT = 0.03;
	static private final long T_GAP = 60 * 60 * 1000; // 1 hour
	
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
	}
	
	static private boolean join(OnlineEvent event, Burst burst){
		if(burst.getTime().after(new Timestamp(event.getEnd().getTime() + T_GAP))){
			return false;
		}
		
		double s = 0.0;
		double s2 = 0.0;
		
		for(Map.Entry<String, Double> entry : burst.getDistribution().entrySet()){
			String word = entry.getKey();
			double p = entry.getValue();
			if(event.getKeywords().contains(word)){
				s += p;
			}else{
				s2 += p;
			}
		}
		/*
		for(String word : event.getKeywords()){
			Double p = burst.getDistribution().get(word);
			if(p == null)	continue;
			
			s += p;
		}*/
		return  s > JOIN_THRESHOLD;
	}
	
	static public void join(List<OnlineEvent> events, List<Burst> bursts){
		bursts = BurstCompare.merge(bursts);
		
		List<Burst> toBeAdded = new LinkedList<Burst>();
		
		for(Burst burst : bursts){
			boolean joined = false;
			for(OnlineEvent event : events){
				if(join(event, burst)){
					event.add(burst);
					joined = true;
					break;
				}
			}
			if(!joined){
				toBeAdded.add(burst);
			}
		}
		
		for(Burst burst : toBeAdded){
			//events.add(new OnlineEvent(burst));
			OnlineEvent event = new OnlineEvent(burst);
			if(event.getKeywords().size() > 0){
				events.add(event);
			}
		}
		
	}

}
