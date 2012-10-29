package linegroup3.tweetstream.rt2.sket;

import java.sql.Timestamp;

public class Estimator {
	private Sketch sketch1 = null;
	
	private Sketch sketch2 = null;
	
	private Timestamp t1 = null;
	
	private Timestamp t2 = null;
	
	private Timestamp t = null;
	
	
	double w1 = 0;
	
	double w2 = 0;
	
	public Estimator(Sketch sketch1, Sketch sketch2, Timestamp t) throws Exception{
		this.sketch1 = sketch1;
		this.sketch2 = sketch2;
		this.t = t;
		
		t1 = sketch1.getTime();
		t2 = sketch2.getTime();
		
		if(t.after(t2) || t.before(t1)){ throw new Exception("Time t is out of range!");}
		
		long dt = t2.getTime() - t1.getTime();
		long dt1 = t.getTime() - t1.getTime();
		long dt2 = t2.getTime() - t.getTime();
		
		w1 = (double)dt1/dt;
		w2 = (double)dt2/dt;
	}
	
	public Pair zeroOrder(){
		if(t.equals(t1)) return zeroOrder(sketch1);
		if(t.equals(t2)) return zeroOrder(sketch2);
		
		Pair p1 = zeroOrder(sketch1);
		Pair p2 = zeroOrder(sketch2);
		
		return estimate(p1, p2, w1, w2);
	}
	
	public Pair zeroOrderDiff(Sketch sketch){
		
		Pair p = zeroOrder(sketch);
		Pair p1 = zeroOrder(sketch1);
		Pair p2 = zeroOrder(sketch2);
		
		return estimateDiff(p, p1, p2, w1, w2);
	}
	
	public Pair[][] firstOrder(){
		if(t.equals(t1)) return firstOrder(sketch1);
		if(t.equals(t2)) return firstOrder(sketch2);

		
		Pair[][] p1 = firstOrder(sketch1);
		Pair[][] p2 = firstOrder(sketch2);
		
		int H = Sketch.H;
		int N = Sketch.N;
		
		Pair[][] ret = new Pair[H][N];
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				ret[h][i] = estimate(p1[h][i], p2[h][i], w1, w2);
			}
		}
		
		return ret;
	}
	
	public Pair[][] firstOrderDiff(Sketch sketch){

		Pair[][] p = firstOrder(sketch);
		Pair[][] p1 = firstOrder(sketch1);
		Pair[][] p2 = firstOrder(sketch2);
		
		int H = Sketch.H;
		int N = Sketch.N;
		
		Pair[][] ret = new Pair[H][N];
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				ret[h][i] = estimateDiff(p[h][i], p1[h][i], p2[h][i], w1, w2);
			}
		}
		
		return ret;
	}
	
	public Pair[][][] secondOrder(){
		if(t.equals(t1)) return secondOrder(sketch1);
		if(t.equals(t2)) return secondOrder(sketch2);
		
		Pair[][][] p1 = secondOrder(sketch1);
		Pair[][][] p2 = secondOrder(sketch2);
		
		int H = Sketch.H;
		int N = Sketch.N;
		
		Pair[][][] ret = new Pair[H][N][N];
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				for(int j = 0; j < N; j ++){
					ret[h][i][j] = estimate(p1[h][i][j], p2[h][i][j], w1, w2);
				}	
			}
		}
		
		return ret;
	}
	
	public Pair[][][] secondOrderDiff(Sketch sketch){
		
		Pair[][][] p = secondOrder(sketch);
		Pair[][][] p1 = secondOrder(sketch1);
		Pair[][][] p2 = secondOrder(sketch2);
		
		int H = Sketch.H;
		int N = Sketch.N;
		
		Pair[][][] ret = new Pair[H][N][N];
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				for(int j = 0; j < N; j ++){
					ret[h][i][j] = estimateDiff(p[h][i][j], p1[h][i][j], p2[h][i][j], w1, w2);
				}	
			}
		}
		
		return ret;
	}
	
	private Pair zeroOrder(Sketch sketch){
		Timestamp st = sketch.getTime();
		return sketch.zeroOrder.get(st);
	}
	
	private Pair[][] firstOrder(Sketch sketch){
		int H = Sketch.H;
		int N = Sketch.N;
		
		Pair[][] ret = new Pair[H][N];
		
		Timestamp st = sketch.getTime();
		
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				ret[h][i] = sketch.firstOrder[h][i].get(st);
			}
		}
		
		return ret;
	}
	
	private Pair[][][] secondOrder(Sketch sketch){
		int H = Sketch.H;
		int N = Sketch.N;
		
		Pair[][][] ret = new Pair[H][N][N];
		
		Timestamp st = sketch.getTime();
		
		for(int h = 0; h < H; h ++){
			for(int i = 0; i < N; i ++){
				for(int j = 0; j < N; j ++){
					ret[h][i][j] = sketch.secondOrder[h][i][j].get(st);
				}
			}
		}
		
		return ret;
	}
	
	private Pair estimate(Pair p1, Pair p2, double w1, double w2){// t1, w1, t, w2, t2
		return new Pair(w2 * p1.v + w1 * p2.v, w2 * p1.a + w1 * p2.a);
	}
	
	private Pair estimateDiff(Pair p, Pair p1, Pair p2, double w1, double w2){// t1, w1, t, w2, t2
		return new Pair(p.v - (w2 * p1.v + w1 * p2.v), p.a - (w2 * p1.a + w1 * p2.a));
	}
}
