package linegroup3.tweetstream.rt;

import java.sql.Timestamp;

public class Order_Sketch {
	static private Timestamp t0 = new Timestamp(0);

	public int H = 0;
	public int N = 0;
	
	private Timestamp observedTime = null;
	
	public Sketch_Pair zeroOrder = null; // H 
	
	public Sketch_Pair[][] firstOrder = null; // H*N
	
	public Sketch_Pair[][][] secondOrder = null; // H*N*N
	
	
	public Order_Sketch(int H, int N){
		this.H = H;
		this.N = N;
		
		zeroOrder = new Sketch_Pair(t0, 0);
		
		firstOrder = new Sketch_Pair[H][N];
		secondOrder = new Sketch_Pair[H][N][N];
		
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				firstOrder[h][i] = new Sketch_Pair(t0, 0);
				
				for(int j = 0; j < N; j ++){
					secondOrder[h][i][j] = new Sketch_Pair(t0, 0);
				}
			}
		}
		
		
		
	}
	
	public void copy(Order_Sketch ret){
		ret.H = H;
		ret.N = N;
		ret.observe(observedTime);
		
		ret.zeroOrder = zeroOrder;
		
		for(int h = 0; h < H; h ++){
			
			for(int i = 0; i < N; i ++){
				ret.firstOrder[h][i] = firstOrder[h][i];
				
				for(int j = 0; j < N; j ++){
					ret.secondOrder[h][i][j] = secondOrder[h][i][j];
				}
			}
		}
		

	}
	
	public void observe(Timestamp time){
		observedTime = time;
	}
	
	public Timestamp getObservedTime(){
		return observedTime;
	}
	
	public double zeroOrderPulse(Timestamp currentTime, double change, long smooth){
		double dt = currentTime.getTime() - observedTime.getTime();
		double e = 1;
		if(smooth != 0) e = Math.exp(-dt/smooth);
		
		double value = zeroOrder.getValue(observedTime, smooth);
		double newV = value*e + change;
		double d = newV - value;
		
		zeroOrder = new Sketch_Pair(currentTime, newV);
		return d;
	}
	
	public double firstOrderPulse(Timestamp currentTime, double change, long smooth, int h, int i){
		double dt = currentTime.getTime() - observedTime.getTime();
		double e = 1;
		if(smooth != 0) e = Math.exp(-dt/smooth);
		
		double value = firstOrder[h][i].getValue(observedTime, smooth);
		double newV = value*e + change;
		double d = newV - value;
		
		firstOrder[h][i] = new Sketch_Pair(currentTime, newV);
		return d;
	}
	
	
	public double secondOrderPulse(Timestamp currentTime, double change, long smooth, int h, int i, int j){
		double dt = currentTime.getTime() - observedTime.getTime();
		double e = 1;
		if(smooth != 0) e = Math.exp(-dt/smooth);
		
		double value = secondOrder[h][i][j].getValue(observedTime, smooth);
		double newV = value*e + change;
		double d = newV - value;
		
		secondOrder[h][i][j] = new Sketch_Pair(currentTime, newV);
		return d;
	}
}
