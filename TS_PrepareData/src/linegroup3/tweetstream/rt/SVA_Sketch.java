package linegroup3.tweetstream.rt;

import java.sql.Timestamp;

public class SVA_Sketch {
	private int H = 0;
	private int N = 0;
	
	public Order_Sketch s = null;
	
	public Order_Sketch v = null;
	
	public Order_Sketch a = null;
	
	public SVA_Sketch(int H, int N){
		this.H = H;
		this.N = N;
		
		s = new Order_Sketch(H, N);
		v = new Order_Sketch(H, N);
		a = new Order_Sketch(H, N);
	}
	
	public SVA_Sketch copy(){
		SVA_Sketch ret = new SVA_Sketch(H, N);
		
		ret.s = s.copy();
		ret.v = v.copy();
		ret.a = a.copy();
		
		return ret;
	}
	
	public void observe(Timestamp time){
		s.observe(time);
		v.observe(time);
		a.observe(time);
	}
}
