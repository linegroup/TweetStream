package linegroup3.tweetstream.rt;

import java.sql.Timestamp;

public class Order_Sketch {
	static private Timestamp t0 = new Timestamp(0);

	private int H = 0;
	private int N = 0;
	
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
	
	public Order_Sketch copy(){
		Order_Sketch ret = new Order_Sketch(H, N);
		
		ret.zeroOrder = zeroOrder.copy();
		
		for(int h = 0; h < H; h ++){
			
			for(int i = 0; i < N; i ++){
				ret.firstOrder[h][i] = firstOrder[h][i].copy();
				
				for(int j = 0; j < N; j ++){
					ret.secondOrder[h][i][j] = secondOrder[h][i][j].copy();
				}
			}
		}
		
		return ret;
	}
	
	public void observe(Timestamp time){
		observedTime = time;
	}
	
	public Timestamp getObservedTime(){
		return observedTime;
	}
}
