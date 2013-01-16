package linegroup3.tweetstream.postprocess;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import linegroup3.tweetstream.rt2.StopWords;

import cmu.arktweetnlp.Twokenize;

public class GatherRelatedTweets {
	
	
	public void gather(Iterable<ValueTermPair> topic, Timestamp t){
		
	}
	
	
	private double score(String tweet, Iterable<ValueTermPair> topic){
		////////////////////////////////////////////
		tweet = decode(tweet);
		tweet = downcase(tweet);
		List<String> terms = tokenize(tweet);
		
		List<String> finalTerms = new LinkedList<String>();
		for(String term : terms){
			if(!StopWords.isStopWord(term)){
				finalTerms.add(term);
			}		
		}
		////////////////////////////////////////////
		
		Map<String, Double> pMap = new TreeMap<String, Double>();
		for(ValueTermPair pair : topic){
			pMap.put(pair.term, pair.v);
		}
		
		double s = 0.0;
		for(String term : finalTerms){
			Double p = pMap.get(term);
			
			if(p == null) continue;
			
			s += Math.log(p);
		}
		
		return Math.exp(s / 10);
	}
	
	/*
	private double score(String tweet, Iterable<ValueTermPair> topic){
		////////////////////////////////////////////
		tweet = decode(tweet);
		tweet = downcase(tweet);
		List<String> terms = tokenize(tweet);
		
		List<String> finalTerms = new LinkedList<String>();
		for(String term : terms){
			if(!StopWords.isStopWord(term)){
				finalTerms.add(term);
			}		
		}
		////////////////////////////////////////////
		
		if(finalTerms.size() == 0) return 0;
		
		double maxFreq = 1.0;
		Map<String, Double> tfMap = new TreeMap<String, Double>();
		for(String term : finalTerms){
			if(tfMap.containsKey(term)){
				double freq = tfMap.get(term);
				if(maxFreq <= freq){
					maxFreq = freq + 1;
				}
				tfMap.put(term, tfMap.get(term) + 1);
			}else{
				tfMap.put(term, 1.0);
			}
		}
		
		double s = 0.0;
		
		for(ValueTermPair pair : topic){
			Double freq = tfMap.get(pair.term);
			if(freq == null) continue;
			
			//s += 
		}
		
		
		
		
		return s;
	}
	*/
	
	/////////////////////////////////////////////////////////////////////////
	
	private static String decode(String tweet){
		return org.apache.commons.lang.StringEscapeUtils.unescapeHtml(tweet);
	}
	
	private static String downcase(String tweet) {
		return tweet.toLowerCase();
	}
	
	private static List<String> tokenize(String tweet) {
		List<String> ret = new LinkedList<String>();
		
		String str = tweet;
		str = str.replaceAll("\\.{10,}+", " ");
		List<String> terms = Twokenize.tokenize(str);

		
		final String regex = "\\p{Punct}+";
		for (String term : terms) {
			if (term.length() > 0 && term.length() <= 64
					&& !term.matches(regex)) {
				ret.add(term);
			}
		}
		

		return ret;

	}

}
