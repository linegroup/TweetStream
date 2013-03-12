package linegroup3.tweetstream.io.output;

import java.util.List;

import linegroup3.tweetstream.postprocess.TokenizeTweet;


public class Matcher implements TweetMatch{

	@Override
	public boolean match(String tweet, String[] keywords) { // length of keywords should >= 3
		if(keywords.length < 3) return false;
		
		String topword = keywords[0];
		
		List<String> terms = TokenizeTweet.tokenizeTweet(tweet);
		
		if(terms.contains(topword))	return true;
		
		int count = 0;
		for(String keyword : keywords){
			if(terms.contains(keyword))	count ++;
		}
		if(count >= 2) return true;
		
		return false;
	}

}
