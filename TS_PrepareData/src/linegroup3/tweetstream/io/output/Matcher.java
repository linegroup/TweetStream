package linegroup3.tweetstream.io.output;

import java.util.List;

import linegroup3.tweetstream.postprocess.TokenizeTweet;


public class Matcher implements TweetMatch{

	
	@Override
	public boolean match(String tweet, String[] keywords) { 
		if(keywords.length < 1) return false;
		
		String topword = keywords[0];
		
		List<String> terms = TokenizeTweet.tokenizeTweet(tweet);
		
		if(terms.contains(topword))	{
			if(Math.random() <= 0.3){
				return true;
			}
		}
		
		int count = 0;
		for(String keyword : keywords){
			if(terms.contains(keyword))	count ++;
		}
		if(count >= 2) return true;
		
		return false;
	}

	/*
	@Override
	public boolean match(String tweet, OnlineEvent event) {
		
		List<String> terms = TokenizeTweet.tokenizeTweet(tweet);
		
		if(terms.size() == 0)	return false;
		
		double s = 0.0;
		for(String term : terms){
			s += event.support(term);
		}
			
		s /= Math.log(1 + terms.size());
		
		return s >= 0.020;
		
		//return false;
	}*/
	
	/*
	@Override
	public boolean match(String tweet, OnlineEvent event) {
		
		List<String> terms = TokenizeTweet.tokenizeTweet(tweet);
		
		if(terms.size() == 0)	return false;
		
		double s = 0.0;
		for(String term : terms){
			s += event.support(term) * FreqMaintainer.idf(term);
		}
			
		s /= Math.log(5 + terms.size());
		
		return s >= 0.3;
		
		//return false;
	}*/

}
