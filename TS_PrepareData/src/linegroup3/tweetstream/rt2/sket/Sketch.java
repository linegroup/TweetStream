package linegroup3.tweetstream.rt2.sket;

import java.sql.Timestamp;


public class Sketch {
	/////// SET SMOOTH HERE !!!
	static final long smooth_1 = 15; // minute
	static final long smooth_2 = 5; // minute
	static final long oneMinute = 60 * 1000; // (ms)
	
	/////// SET H & K HERE !!!
	static final int H = 5;
	static final int N = 200;
	
	static private Timestamp t0 = new Timestamp(0);

	
	private Timestamp MainTstamp = null;
	
	public Unit zeroOrder = null;
	
	public Unit[][] firstOrder = null;
	
	public Unit[][][] secondOrder = null;
		
	public Sketch(){
		MainTstamp = t0;
		
		zeroOrder = new Unit(t0, 0, 0);	
		firstOrder = new Unit[H][N];
		secondOrder = new Unit[H][N][N];
		
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				firstOrder[h][i] = new Unit(t0, 0, 0);
				
				for(int j = 0; j < N; j ++){
					secondOrder[h][i][j] = new Unit(t0, 0, 0);
				}
			}
		}
		
	}
	
	public void copy(Sketch ret){
		ret.observe(MainTstamp);
		
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
		MainTstamp = time;		
	}
	
	public Timestamp getTime(){
		return MainTstamp;
	}
	
	public void zeroOrderPulse(Timestamp currentTime, double ds){
		double dt = currentTime.getTime() - MainTstamp.getTime();
		
		double e_1 = 1.0;
		double e_2 = 1.0;
		
		if(dt != 0){
			e_1 = Math.exp(-dt/(smooth_1*oneMinute));
			e_2 = Math.exp(-dt/(smooth_2*oneMinute));
		}
		
		Pair pair = zeroOrder.get(MainTstamp);
		
		double newV1 = e_1*pair.v + ds / smooth_1;
		double newV2 = e_2*(pair.v + pair.a) + ds / smooth_2;
		
		zeroOrder = new Unit(currentTime, newV1, newV2);

	}
	
	public void firstOrderPulse(Timestamp currentTime, double ds, int h, int i){
		double dt = currentTime.getTime() - MainTstamp.getTime();
		
		double e_1 = 1.0;
		double e_2 = 1.0;
		
		if(dt != 0){
			e_1 = Math.exp(-dt/(smooth_1*oneMinute));
			e_2 = Math.exp(-dt/(smooth_2*oneMinute));
		}
		
		Pair pair = firstOrder[h][i].get(MainTstamp);
		
		double newV1 = e_1*pair.v + ds / smooth_1;
		double newV2 = e_2*(pair.v + pair.a) + ds / smooth_2;
		
		firstOrder[h][i] = new Unit(currentTime, newV1, newV2);
	}
	
	
	public void secondOrderPulse(Timestamp currentTime, double ds, int h, int i, int j){
		double dt = currentTime.getTime() - MainTstamp.getTime();
		
		double e_1 = 1.0;
		double e_2 = 1.0;
		
		if(dt != 0){
			e_1 = Math.exp(-dt/(smooth_1*oneMinute));
			e_2 = Math.exp(-dt/(smooth_2*oneMinute));
		}
		
		Pair pair = secondOrder[h][i][j].get(MainTstamp);
		
		double newV1 = e_1*pair.v + ds / smooth_1;
		double newV2 = e_2*(pair.v + pair.a) + ds / smooth_2;
		
		secondOrder[h][i][j] = new Unit(currentTime, newV1, newV2);
	}
	
	
	public class Unit{
		private Timestamp Tstamp = null;
		private double v1 = 0;
		private double v2 = 0;
		
		private Unit(Timestamp t, double v1, double v2){
			this.Tstamp = t;
			this.v1 = v1;
			this.v2 = v2;
		}
		
		public Pair get(Timestamp time){			
			double dt = time.getTime() - Tstamp.getTime();
			
			if(dt == 0)	return new Pair(v1, v2 - v1);
			
			double e_1 = Math.exp(-dt/(smooth_1*oneMinute));
			double e_2 = Math.exp(-dt/(smooth_2*oneMinute));
			
			double currentV1 = v1*e_1;
			double currentV2 = v2*e_2;

			
			return new Pair(currentV1, currentV2 - currentV1);
		}	

	}
	
	
	
}
