package linegroup3.tweetstream.event;

import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;

import linegroup3.common.Config;


public class Burst {
	
	private Timestamp time = null;
	
	private double optima = 0.0;
	
	private Map<String, Double> distribution = new TreeMap<String, Double>();
	
	public Burst(JSONObject obj){
		try {
			time = Timestamp.valueOf(obj.getString("t"));
			optima = obj.getDouble("op");
			JSONObject p = obj.getJSONObject("p");
			for(String word : JSONObject.getNames(p)){
				distribution.put(word, p.getDouble(word));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
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
		if(!Config.cos_sim)	return 1.0;
		
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
