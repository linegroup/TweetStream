package linegroup3.tweetstream.event;

import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;


public class Burst {
	
	private Timestamp time = null;
	
	private double optima = 0.0;
	
	private Map<String, Double> distribution = new TreeMap<String, Double>();
	
	
	public Burst(Timestamp time, double optima){
		this.time = time;
		this.optima = optima;
	}
	
	public void prob(String word, double p){
		distribution.put(word, p);
	}
	
	public Timestamp getTime(){
		return time;
	}
	
	public Map<String, Double> getDistribution(){
		return distribution;
	}
	
	public double getOptima(){
		return optima;
	}
	
	public double similarity(Burst burst){
		double s = 0.0;
		for(String word : distribution.keySet()){
			Double p = burst.getDistribution().get(word);
			if(p == null)	continue;
			
			s += distribution.get(word) * p;
		}
		
		return s / (norm() * burst.norm());
	}
	
	public double norm(){
		double ret = 0.0;
		for(String word : distribution.keySet()){
			double p = distribution.get(word);
			ret += p * p;
		}
		return Math.sqrt(ret);
	}

	
	public String toString(){
		String ret = "time:" + time + "\n";
		ret += "optima:" + optima + "\n";
		for(Map.Entry<String, Double> entry : distribution.entrySet()){
			ret += entry.getKey() + "\t" + entry.getValue() + "\n";
		}
		return ret;
	}

}
