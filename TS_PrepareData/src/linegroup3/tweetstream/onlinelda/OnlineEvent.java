package linegroup3.tweetstream.onlinelda;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import linegroup3.tweetstream.postprocess.ValueTermPair;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class OnlineEvent {
	private Timestamp start = null;
	private Timestamp end = null;
	private int topicId = -1;

	private List<Burst> bursts = new LinkedList<Burst>();
	
	public OnlineEvent(Burst burst){
		start = burst.getStartTime();
		end = burst.getEndTime();
		topicId = burst.getTopicId();
		add(burst);
	}
	
	public void add(Burst burst){// must add busts in order by time
		end = burst.getEndTime();
		
		bursts.add(burst);

	}
	
	public int getTopicId(){
		return topicId;
	}
	
	public Timestamp getStart(){
		return start;
	}
	
	public Timestamp getEnd(){
		return end;
	}
	
	public List<String> getKeywords(){
		Set<String> words = new TreeSet<String>();
		for(Burst burst : bursts){
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
		
		int TOP_N = 5;
		List<String> ret = new LinkedList<String>();
		for(i = 0; i < n && i < TOP_N; i ++){
			ret.add(pairs[i].term);
		}
		
		return ret;
	}
	
	public List<Burst> getBursts(){
		return bursts;
	}
	
	public String toString(){
		String ret = "[" + start + "," + end + "]\t";
		for(String word : getKeywords()){
			ret += (word + ",");
		}
		ret += "\t:";
		for(String word : allwords()){
			ret += (word + ",");
		}
		return ret;
	}
	
	public double support(String word){
		double s = 0.0;
		for(Burst burst : bursts){
			Double p = burst.getDistribution().get(word);
			if(p != null){
				s += p;
			}
		}	
		
		return s / bursts.size();
	}
	
	public List<String> allwords(){ // all the words in order by weight
		Set<String> words = new TreeSet<String>();
		for(Burst burst : bursts){
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
		for(Burst b : bursts){
			JSONObject burst = new JSONObject();
			JSONObject obj = new JSONObject(b.getDistribution());
			burst.put("p", obj);
			burst.put("dt", b.getDetectionTime());
			burst.put("s", b.getStartTime());
			burst.put("e", b.getEndTime());
			burst.put("id", b.getTopicId());
			ret.put(burst);
		}
		return ret;
	}
}
