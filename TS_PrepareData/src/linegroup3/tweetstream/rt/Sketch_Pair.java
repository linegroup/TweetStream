package linegroup3.tweetstream.rt;

import java.sql.Timestamp;

public class Sketch_Pair {
	private double value = 0;
	private Timestamp time = null;
	
	public Sketch_Pair(Timestamp time, double value){
		this.value = value;
		this.time = time;
	}
	
	/*  NO COPY !!!!!!! LAZY COPY !!!!!! NO MODIFY !!!!!
	public Sketch_Pair copy(){
		Sketch_Pair ret = new Sketch_Pair(modifiedTime, value);
		return ret;
	}
	
	private void modify(Timestamp currentTime, double v){
		modifiedTime = currentTime;
		value = v;
	}
	*/
	
	public double getValue() { return value; }
	
	public Timestamp getTime()  { return time; }
	
	public double getValue(Timestamp currentTime, long smooth){
		double dt = currentTime.getTime() - time.getTime();
		double e= Math.exp(dt / smooth);
		return e*value;
	}
	
}
