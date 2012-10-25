package linegroup3.tweetstream.rt2.sket;

import java.sql.Timestamp;

public class OutputSketch {
	public static String outputZeroOrder(Sketch sketch){
		StringBuilder sb = new StringBuilder();
		Timestamp t = sketch.getTime();
		Sketch.Pair pair = sketch.zeroOrder.get(t);
		sb.append("V:\t" + pair.v + "\n");
		sb.append("A:\t" + pair.a + "\n");
		return sb.toString();
	}
	
	public static String[] outputFirstOrderV(Sketch sketch){
		int H = Sketch.H;
		int N = Sketch.N;
		Timestamp t = sketch.getTime();
		String[] ret = new String[H];
		for(int h = 0; h < H; h ++){
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < N; i ++){
				Sketch.Pair pair = sketch.firstOrder[h][i].get(t);
				sb.append("\t" + pair.v);
			}
			sb.append("\n");
			ret[h] = sb.toString();
		}
		return ret;
	}
	
	
	public static String[] outputFirstOrderA(Sketch sketch){
		int H = Sketch.H;
		int N = Sketch.N;
		Timestamp t = sketch.getTime();
		String[] ret = new String[H];
		for(int h = 0; h < H; h ++){
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < N; i ++){
				Sketch.Pair pair = sketch.firstOrder[h][i].get(t);
				sb.append("\t" + pair.a);
			}
			sb.append("\n");
			ret[h] = sb.toString();
		}
		return ret;
	}
	
	public static String[] outputSecondOrderV(Sketch sketch){
		int H = Sketch.H;
		int N = Sketch.N;
		Timestamp t = sketch.getTime();
		String[] ret = new String[H];
		for(int h = 0; h < H; h ++){
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < N; i ++){
				for(int j = 0; j < N; j ++){
					Sketch.Pair pair = sketch.secondOrder[h][i][j].get(t);
					sb.append("\t" + pair.v);
				}
				sb.append("\n");
			}
			
			ret[h] = sb.toString();
		}
		return ret;
	}
	
	public static String[] outputSecondOrderA(Sketch sketch){
		int H = Sketch.H;
		int N = Sketch.N;
		Timestamp t = sketch.getTime();
		String[] ret = new String[H];
		for(int h = 0; h < H; h ++){
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < N; i ++){
				for(int j = 0; j < N; j ++){
					Sketch.Pair pair = sketch.secondOrder[h][i][j].get(t);
					sb.append("\t" + pair.a);
				}
				sb.append("\n");
			}
			
			ret[h] = sb.toString();
		}
		return ret;
	}
	
}
