package linegroup3.tweetstream.rt2.sketch;

import java.sql.Timestamp;
import java.util.TreeSet;


public class Sketch {
	/////// SET SMOOTH HERE !!!
	static final long smooth_V = 15; // minute
	static final long smooth_A = 5; // minute
	static final long oneMinute = 60 * 1000; // (ms)
	
	/////// SET H & K HERE !!!
	static final int H = 5;
	static final int N = 200;
	
	static private Timestamp t0 = new Timestamp(0);

	
	private Timestamp MainTstamp = null;
	
	public Unit zeroOrder = null;
	
	public Unit[][] firstOrder = null;
	
	public Unit[][][] secondOrder = null;
	
	static private TreeSet<Timestamp> checkpoints = new TreeSet<Timestamp>();
	
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
		
		checkpoints.add(time);
	}
	
	public Timestamp getTime(){
		return MainTstamp;
	}
	
	public void zeroOrderPulse(Timestamp currentTime, double ds){
		double dt = currentTime.getTime() - MainTstamp.getTime();
		
		double e_v = Math.exp(-dt/(smooth_V*oneMinute));
		double e_a = Math.exp(-dt/(smooth_A*oneMinute));
		
		Pair pair = zeroOrder.get(MainTstamp);
		
		double newV = e_v*pair.v + ds / smooth_V;
		double dv = newV - pair.v;
		double newA = e_a*pair.a + dv / smooth_A;
		
		zeroOrder = new Unit(currentTime, newV, newA);

	}
	
	public void firstOrderPulse(Timestamp currentTime, double ds, int h, int i){
		double dt = currentTime.getTime() - MainTstamp.getTime();
		
		double e_v = Math.exp(-dt/(smooth_V*oneMinute));
		double e_a = Math.exp(-dt/(smooth_A*oneMinute));
		
		Pair pair = firstOrder[h][i].get(MainTstamp);
		
		double newV = e_v*pair.v + ds / smooth_V;
		double dv = newV - pair.v;
		double newA = e_a*pair.a + dv / smooth_A;
		
		firstOrder[h][i] = new Unit(currentTime, newV, newA);
	}
	
	
	public void secondOrderPulse(Timestamp currentTime, double ds, int h, int i, int j){
		double dt = currentTime.getTime() - MainTstamp.getTime();
		
		double e_v = Math.exp(-dt/(smooth_V*oneMinute));
		double e_a = Math.exp(-dt/(smooth_A*oneMinute));
		
		Pair pair = secondOrder[h][i][j].get(MainTstamp);
		
		double newV = e_v*pair.v + ds / smooth_V;
		double dv = newV - pair.v;
		double newA = e_a*pair.a + dv / smooth_A;
		
		secondOrder[h][i][j] = new Unit(currentTime, newV, newA);
	}
	
	
	public class Unit{
		private Timestamp Tstamp = null;
		private double v = 0;
		private double a = 0;
		
		public Unit(Timestamp t, double v, double a){
			this.Tstamp = t;
			this.v = v;
			this.a = a;
		}
		
		public Pair get(Timestamp time){			
			double dt = time.getTime() - Tstamp.getTime();
			
			if(dt == 0)	return new Pair(v, a);
			
			double e_v = Math.exp(-dt/(smooth_V*oneMinute));
			double e_a = Math.exp(-dt/(smooth_A*oneMinute));
			
			double currentV = v*e_v;
			
			/////////////////////////////////
			Timestamp initialPiont = Tstamp;
			double initialA = a;
			double initialV = v;
			for(Timestamp timepoint :Sketch.checkpoints.subSet(Tstamp, time)){
				if(timepoint.after(Tstamp)){
					dt = timepoint.getTime() - initialPiont.getTime();
					
					e_v = Math.exp(-dt/(smooth_V*oneMinute));
					e_a = Math.exp(-dt/(smooth_A*oneMinute));
					
					double tempV = initialV*e_v;
					double tempA = initialA*e_a + (tempV - initialV) / smooth_A ;
					
					initialV = tempV;
					initialA = tempA;
					initialPiont = timepoint;
				}
			}
			
			dt = time.getTime() - initialPiont.getTime();
			e_a = Math.exp(-dt/(smooth_A*oneMinute));
			double dv = currentV - initialV;
			double currentA = initialA*e_a + dv / smooth_A;
			
			return new Pair(currentV, currentA);
		}	

	}		
	
}
