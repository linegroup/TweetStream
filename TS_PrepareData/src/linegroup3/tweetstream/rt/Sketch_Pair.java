package linegroup3.tweetstream.rt;

import java.sql.Timestamp;

public class Sketch_Pair {
	public double value = 0;
	public Timestamp modifiedTime = null;
	
	public Sketch_Pair(Timestamp modifiedTime, double value){
		this.value = value;
		this.modifiedTime = modifiedTime;
	}
	
	public Sketch_Pair copy(){
		Sketch_Pair ret = new Sketch_Pair(modifiedTime, value);
		return ret;
	}
	
	private void modify(Timestamp currentTime, double v){
		modifiedTime = currentTime;
		value = v;
	}
	
	public double pulse(Timestamp currentTime, double change, long smooth){
		double dt = currentTime.getTime()-modifiedTime.getTime();
		double e = 1;
		if(smooth != 0) e = Math.exp(-dt/smooth);
		
		double newV = value*e + change;
		double d = newV - value;
		
		modify(currentTime, newV);
		return d;
	}
	
}
