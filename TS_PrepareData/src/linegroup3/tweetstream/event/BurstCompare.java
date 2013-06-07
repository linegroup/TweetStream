package linegroup3.tweetstream.event;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import linegroup3.common.Config;

public class BurstCompare {
	static private final double SIMILARITY_THRESHOLD = Config.SIMILARITY_THRESHOLD;
	static private final double JOIN_THRESHOLD = Config.JOIN_THRESHOLD;
	static public final double MIN_SUPPORT = 0.025;
	static public final long T_GAP = 60 * 60 * 1000; // 1 hour


	static private List<Burst> merge(List<Burst> bursts){
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
		/*
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
		}*/
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
	
	
	
	/////////////////////////////////////////////////////////
	
	static private double join(OnlineEvent event, Burst burst){
		if(burst.getTime().after(new Timestamp(event.getEnd().getTime() + T_GAP))){
			return 0.0;
		}
		
		double s = 0.0;
		
		for(Map.Entry<String, Double> entry : burst.getDistribution().entrySet()){
			String word = entry.getKey();
			double p = entry.getValue();
			s += p * event.support(word);
		}

		return  s / (event.norm() * burst.norm());
	}
	
	static public void join(List<OnlineEvent> events, List<Burst> bursts){
		int n = bursts.size();		
		if(n > 1){
			bursts = BurstCompare.merge(bursts);
			n = bursts.size();
		}
		
		int m = events.size();
		if(m == 0){
			for(Burst burst : bursts){
				OnlineEvent event = new OnlineEvent(burst);
				if (event.getKeywords().size() > 0) {
					events.add(event);
				}
			}
			return;
		}
		
		
		int i = 0;
		Burst[] bs = new Burst[n];
		double[] values = new double[n];
		int[] index = new int[n];
		for(Burst burst : bursts){
			bs[i] = burst;
			values[i] = 0.0;
			index[i] = -1;
			i ++;
		}
		
		int j = 0;
		OnlineEvent[] es = new OnlineEvent[m];
		for(OnlineEvent event : events){
			es[j] = event;
			j ++;
		}
		
		for(i = 0; i < n; i ++){
			for(j = 0; j < m; j ++){
				double s = join(es[j], bs[i]);
				if(s > values[i]){
					values[i] = s;
					index[i] = j;
				}
			}
		}
		
		for(i = 0; i < n; i ++){
			if(values[i] > JOIN_THRESHOLD){
				es[index[i]].add(bs[i]);
			}else{
				index[i] = -1;
			}
		}
		
		for(i = 0; i < n; i ++){
			if(index[i] == -1){
				OnlineEvent event = new OnlineEvent(bs[i]);
				if (event.getKeywords().size() > 0) {
					events.add(event);
				}
			}
		}
		
		
	}
	
	/*
	static public void join(List<OnlineEvent> events, List<Burst> bursts){
		int n = bursts.size();		
		if(n > 1){
			bursts = BurstCompare.merge(bursts);
			n = bursts.size();
		}
		
		int m = events.size();
		if(m == 0){
			for(Burst burst : bursts){
				OnlineEvent event = new OnlineEvent(burst);
				if (event.getKeywords().size() > 0) {
					events.add(event);
				}
			}
			return;
		}
		
		
		int i = 0;
		Burst[] bs = new Burst[n];
		for(Burst burst : bursts){
			bs[i] = burst;
			i ++;
		}
		
		int j = 0;
		OnlineEvent[] es = new OnlineEvent[m];
		double[] values = new double[m];
		int[] index = new int[m];
		for(OnlineEvent event : events){
			es[j] = event;
			values[j] = 0.0;
			index[j] = -1;
			j ++;
		}
		
		for(i = 0; i < n; i ++){
			for(j = 0; j < m; j ++){
				double s = join(es[j], bs[i]);
				if(s > values[j]){
					values[j] = s;
					index[j] = i;
				}
			}
		}
		
		for(j = 0; j < m; j ++){
			if(values[j] > JOIN_THRESHOLD){
				es[j].add(bs[index[j]]);
			}else{
				index[j] = -1;
			}
		}
		
		for(i = 0; i < n; i ++){
			boolean added = false;
			for(j = 0; j < m; j ++){
				if(index[j] == i){
					added = true;
					break;
				}
			}
			if(!added){
				OnlineEvent event = new OnlineEvent(bs[i]);
				if (event.getKeywords().size() > 0) {
					events.add(event);
				}
			}
		}
		
		
	}*/
	
	/*
	static public void join(List<OnlineEvent> events, List<Burst> bursts){
		int n = bursts.size();
		
		if(n > 1){
			bursts = BurstCompare.merge(bursts);
		}
		
		Burst[] ee = new Burst[n];
		OnlineEvent[] er = new OnlineEvent[n];

		int i = 0;
		for (Burst burst : bursts) {
			ee[i] = burst;
			i++;
		}

		int m = events.size();
		if (m > 0) {
			boolean[] booked = new boolean[m];
			OnlineEvent[] ev = new OnlineEvent[m];
			int j = 0;
			for(OnlineEvent event : events){
				ev[j] = event;
				j ++;
			}

			for (i = 0; i < n; i ++) {
				boolean joined = false;
				for (j = 0; j < m; j ++) {
					if (!booked[j] && join(ev[j], ee[i])) {

						er[i] = ev[j];
						booked[j] = true;
						joined = true;
						break;
					}
				}
				if (!joined) {
					er[i] = null;
				}
			}


		}

		for (i = 0; i < n; i++) {
			if (er[i] != null) {
				er[i].add(ee[i]);
			} else {

				OnlineEvent event = new OnlineEvent(ee[i]);
				if (event.getKeywords().size() > 0) {
					events.add(event);
				}

			}
		}
		
	}*/
	
/*	static private List<String> commonWords(Burst burst, OnlineEvent event){
		List<String> ret = new LinkedList<String>();
		for(String word : burst.getDistribution().keySet()){
			if(event.getKeywords().contains(word)){
				ret.add(word);
			}
		}	
		return ret;
	}*/

}
