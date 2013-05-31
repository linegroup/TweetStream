package linegroup3.tweetstream.postprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import linegroup3.common.Config;

public class SketchCheck {

	private static double[][][] a = null;  // H*N*N
	private static double[][] e = null; // H*N
	private static double Lambda = 0;
	
	private static int N = Config.N;
	private static int H = 5;
	
	public static void checkBatch(String dirpath) throws Exception{
		File dir = new File(dirpath);
		String[] sketchDirStrs = dir.list();
		for (String sketchDirStr : sketchDirStrs) {	
			
			if(sketchDirStr.startsWith("."))	continue;
			
			File sketchDir = new File(dirpath + sketchDirStr);
			if (sketchDir.isDirectory()) {
				check(sketchDir.getAbsolutePath(), true);
			}
		}
	}
	
	public static double check(String sketchDir, boolean print){
		load(sketchDir, 'A');
		
		double min = 1e10;
		for(int h = 0; h < H; h ++){
			double max = -1e10;
			for(int n = 0; n < N; n ++){
				if(e[h][n] > max){
					max = e[h][n];
				}
			}
			if(min > max){
				min = max;
			}
		}
		
		////////////////////////////////////////////////
		if(print){
			String[] res = sketchDir.split("\\\\");
			String str = res[res.length - 1];
		
			String[] res2 = str.split("_");
			String dateStr = res2[0] + "-" + res2[1] + "-" + res2[2] + " " + res2[3] + ":" + res2[4] + ":" + res2[5];
		
			System.out.println(dateStr + "\t" + min);
		}
		////////////////////////////////////////////////
		
		return min;
	}
	
	private static void load(String dir, char c){
		a = null;
		e = null;
		Lambda = 0;
		
		String zeroOrder = "diff_zeroOrder";
		String firstOrder = "diff_firstOrder";
		String secondOrder = "diff_secondOrder";
		switch (c) {
		case 'V': {
			firstOrder += "V_";
			secondOrder += "V_";
		}
			break;
		case 'A': {
			firstOrder += "A_";
			secondOrder += "A_";
		}
			break;
		}
		
		
		try {
			/////// zeroOrder
			BufferedReader in = new BufferedReader(new FileReader(dir + "/" + zeroOrder + ".txt"));
			String line = null;
			while((line = in.readLine()) != null){
				if(line.startsWith("" + c)){
					String[] res = line.split("\t");
					Lambda = Double.parseDouble(res[1]);
				}
			}
			in.close();
			
			/////// firstOrder
			e = new double[H][N];
			for(int h = 0; h < H; h ++){
				in = new BufferedReader(new FileReader(dir + "/" + firstOrder + h + ".txt"));
				line = in.readLine();
				String[] res = line.split("\t");
				for(int i = 0; i < N; i ++){
					e[h][i] = Double.parseDouble(res[i + 1]);
				}
				in.close();
			}
			
			/////// secondOrder
			/*
			a = new double[H][N][N];
			for(int h = 0; h < H; h ++){
				in = new BufferedReader(new FileReader(dir + "/" + secondOrder + h + ".txt"));
				int j = 0;
				while((line = in.readLine()) != null){
					String[] res = line.split("\t");
					for(int i = 0; i < N; i ++){
						a[h][i][j] = Double.parseDouble(res[i + 1]);
					}
					j ++;
				}
				in.close();
			}
			*/

			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}
