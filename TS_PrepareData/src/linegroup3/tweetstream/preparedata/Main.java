package linegroup3.tweetstream.preparedata;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import cmu.arktweetnlp.Twokenize;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//LoadTweetsInFile.statisticMonth();
		
		//StatisticTweets.roughAnalysisPerWeek2();
		
		Preprocess.doCounting();
		
		/*
		String str = "   	  \trt @::.1!!!!2ds\"fds\"fsd,3";
		String[] terms = str.split("[\\s,.!():\"]+");
		for(String term : terms){
			System.out.println(term);
		}
		*/

		/*
		List<String> terms = Twokenize.tokenizeRawTweetText("araphoenix !!! . ! shares http://s3.amazonaws.com/piclyf_pics/16496_308118_b.jpg our new year shirt bit.ly/i4qymr /p/9xig48");
		for(String term : terms){
			if(!term.matches("\\p{Punct}+"))
				System.out.println(term);
		}
		*/
		
		/*
		String str1 = "hellös";
		String str2 = "hellös";
		Map<String, Integer> map = new TreeMap<String, Integer>();
		map.put(str1, 1);
		map.put(str2, 2);
		
		System.out.println(map.size());
		*/
		
		
	}

}
