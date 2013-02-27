package linegroup3.tweetstream.workers;

import java.sql.Timestamp;
import java.util.List;

import linegroup3.tweetstream.rt2.sket.Pair;

public class InferenceUnit {
	public Pair zeroOrderDiff = null;
	
	public Pair[][] firstOrderDiff = null;
	
	public Pair[][][] secondOrderDiff = null;
	
	public List<String> activeTerms = null;
	
	public Timestamp currentTime = null;

}
