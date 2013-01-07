package linegroup3.tweetstream.postprocess;

public class TopicFilter {
	
	public static double minProb = 0.02;
	
	public static boolean filterByOptimization(double F0F1Ratio){
		if(F0F1Ratio < 0.80){
			return false;
		}
		return true;
	}
	
	public static boolean filterByW(double w){
		if(w > 0.225){
			return false;
		}
		return true;
	}
	
	public static boolean filterByTopics(Iterable<ValueTermPair> topic){
		double s = 0.0;
		
		double max = 0.0;
		for(ValueTermPair pair : topic){
			s += pair.v;
			
			if(pair.v > max){
				max = pair.v;
			}
		}
		
		if(s >= 0.05 && max >= 0.04){
			return false;
		}
		
		
		
		
		return true;
	}

}
