package linegroup3.tweetstream.onlinelda;

import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;


public class Burst {
	private int topicId = -1;
	private Timestamp detectionTime = null;
	private Timestamp start = null;
	private Timestamp end = null;
		
	private Map<String, Double> distribution = new TreeMap<String, Double>();
	
	
	public Burst(int topicId, Timestamp detectionTime, Timestamp start, Timestamp end, Map<String, Double> distribution){
		this.topicId = topicId;
		this.detectionTime = detectionTime;
		this.start = start;
		this.end = end;
		this.distribution = distribution;
	}
	
	public void prob(String word, double p){
		distribution.put(word, p);
	}
	
	public int getTopicId(){
		return topicId;
	}
	
	public Timestamp getDetectionTime(){
		return detectionTime;
	}
	
	public Timestamp getStartTime(){
		return start;
	}
	
	public Timestamp getEndTime(){
		return end;
	}
	
	public Map<String, Double> getDistribution(){
		return distribution;
	}
	
	

	

	
	public String toString(){
		String ret = "time:" + detectionTime + "\n";
		ret += "range:[" + start + "," + end + "]" + "\n";
		for(Map.Entry<String, Double> entry : distribution.entrySet()){
			ret += entry.getKey() + "\t" + entry.getValue() + "\n";
		}
		return ret;
	}
}
