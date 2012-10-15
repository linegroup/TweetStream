package linegroup3.tweetstream.rt;

import java.sql.Timestamp;

public class SVA_Sketch {
	static private Timestamp t0 = new Timestamp(0);
	
	public Timestamp time = null;
	
	public Order_Sketch s = null;
	
	public Order_Sketch v = null;
	
	public Order_Sketch a = null;
	
	public SVA_Sketch(int H, int N){
		
		s = new Order_Sketch(H, N);
		v = new Order_Sketch(H, N);
		a = new Order_Sketch(H, N);
		
		observe(t0);
	}
	
	public void copy(SVA_Sketch ret){

		ret.time = time;
		
		s.copy(ret.s);
		v.copy(ret.v);
		a.copy(ret.a);
		
	}
	
	public void observe(Timestamp time){
		this.time = time;
		s.observe(time);
		v.observe(time);
		a.observe(time);
	}
}
