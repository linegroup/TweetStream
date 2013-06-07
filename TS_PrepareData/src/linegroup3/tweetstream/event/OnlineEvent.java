package linegroup3.tweetstream.event;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import linegroup3.common.Config;
import linegroup3.tweetstream.io.output.CacheAgent;
import linegroup3.tweetstream.postprocess.ValueTermPair;

public class OnlineEvent {
	
	private String id = null;
	
	private Timestamp start = null;
	private Timestamp end = null;
	

	private Map<Timestamp, Burst> bursts = new TreeMap<Timestamp, Burst>();
	
	
	private Map<String, Double> distribution = new TreeMap<String, Double>();
	
	public void beginParseTweets(){
		distribution.clear();
	}
	public void parseTweets(String term, double freq){
		Double p = distribution.get(term);
		if(p == null){
			p = new Double(0);
		}
		distribution.put(term, p + freq);
	}
	
	
	public OnlineEvent(Burst burst){
		id = CacheAgent.get().getId();
		
		this.start = burst.getTime();
		add(burst);
		
		CacheAgent.get().put(id, this);
		
		setDistribution();
	}
	
	public String getId(){
		return id;
	}
	
	public void add(Burst burst){// must add busts in order by time
		end = burst.getTime();
		
		bursts.put(burst.getTime(), burst);

		if(Config.self_set_distribution && Config.set_dis_in_add)	setDistribution();
	}
	
	public Timestamp getStart(){
		return start;
	}
	
	public Timestamp getEnd(){
		return end;
	}
	
	
	private void setDistribution(){
		int n = bursts.size();
		for(Burst burst : bursts.values()){
			for(Map.Entry<String, Double> entry : burst.getDistribution().entrySet()){
				String term = entry.getKey();
				Double p = distribution.get(term);
				if(p == null) p = new Double(0);
				distribution.put(term, p + entry.getValue() / n);
			}
		}
	}
	
	public List<String> getKeywords(){
		if(Config.self_set_distribution && Config.set_dis_in_getkeywords)	setDistribution();
		
		int n = distribution.size();
		ValueTermPair[] pairs = new ValueTermPair[n];
		int i = 0;
		for(String word : distribution.keySet()){
			pairs[i] = new ValueTermPair(support(word), word);
			i ++;
		}
		
		Arrays.sort(pairs, new Comparator<ValueTermPair>() {

			@Override
			public int compare(ValueTermPair arg0, ValueTermPair arg1) {
				if (arg0.v > arg1.v)
					return -1;
				if (arg0.v < arg1.v)
					return 1;
				return 0;
			}

		});
		
		int TOP_N = 4;
		List<String> ret = new LinkedList<String>();
		for(i = 0; i < n && i < TOP_N; i ++){
			if(pairs[i].v >= 0.015){
				ret.add(pairs[i].term);
			}
		}
		
		return ret;
	}
	
	public Map<Timestamp, Burst> getBursts(){
		return bursts;
	}
	
	public String toString(){
		String ret = id + "\t" + "[" + start + "," + end + "]\t";
		for(String word : getKeywords()){
			ret += (word + ",");
		}
		ret += "\t:";
		for(String word : allwords()){
			ret += (word + ",");
		}
		return ret;
	}
	
	
	/*
	public double support(String word){
		double s = 0.0;
		for(Burst burst : bursts.values()){
			Double p = burst.getDistribution().get(word);
			if(p != null){
				s += p;
			}
		}	
		
		return s / bursts.size();
	}*/
	
	public double support(String word){
		Double ret =  distribution.get(word);
		if(ret == null) return 0.0;
		return ret;
	}
	
	public double norm(){
		if(!Config.cos_sim)	return 1.0;
			
		double ret = 0.0;
		
		for(double p : distribution.values()){
			ret += p*p;
		}
		
		
		return Math.sqrt(ret);
	}
	
	public List<String> allwords(){ // all the words in order by weight
		Set<String> words = new TreeSet<String>();
		for(Burst burst : bursts.values()){
			for(String word : burst.getDistribution().keySet()){
				words.add(word);
			}
		}
		
		int n = words.size();
		ValueTermPair[] pairs = new ValueTermPair[n];
		int i = 0;
		for(String word : words){
			pairs[i] = new ValueTermPair(support(word), word);
			i ++;
		}
		
		Arrays.sort(pairs, new Comparator<ValueTermPair>() {

			@Override
			public int compare(ValueTermPair arg0, ValueTermPair arg1) {
				if (arg0.v > arg1.v)
					return -1;
				if (arg0.v < arg1.v)
					return 1;
				return 0;
			}

		});
		
		List<String> ret = new LinkedList<String>();
		for(i = 0; i < n; i ++){
			ret.add(pairs[i].term);
		}
		
		return ret;
	}
	
	public String getKeywordsStr(){
		JSONArray array = new JSONArray();
		for(String word : getKeywords()){
			array.put(word);
		}
		return array.toString();
	}
	
	
	public JSONArray getDetail() throws JSONException{
		JSONArray ret = new JSONArray();
		for(Map.Entry<Timestamp, Burst> entry : bursts.entrySet()){
			JSONObject burst = new JSONObject();
			JSONObject obj = new JSONObject(entry.getValue().getDistribution());
			burst.put("p", obj);
			burst.put("t", entry.getKey());
			burst.put("op", entry.getValue().getOptima());
			
			ret.put(burst);
		}
		return ret;
	}
	
	
}
