package linegroup3.tweetstream.inference;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import linegroup3.common.Config;
import linegroup3.tweetstream.preparedata.HashFamily;


public class ParallelInfer {
	private double[][][] a = null;  // H*N*N
	private double[][] e = null; // H*N
	private double Lambda = 0;

	private double[][][] x = null; // guess for topics H*K*N
	private double[] w = null; // guess for lambda, w means weight

	private int MAX_SEARCH_STEP = 300;
	private double M = 1e-1;
	

	private final int N = Config.N;
	private final int K = 5;
	private final int H = 5;
	
	private final ExecutorService pool = Executors.newFixedThreadPool(H);
	private boolean[] states = new boolean[H]; 
	
	private Set<Integer> actives = new TreeSet<Integer>();
	
	public void infer(){
		load("D:/data_for_release2/data/sketch/2011_11_29_04_54_51_0", 'V');
		initial();
		F();
		for (int n = 0; n < 30; n++) {

			for (int h = 0; h < H; h++) {
				setState(h, false);
				pool.execute(new Handler(h));
			}

			while (!getState()) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			searchLambda();
			
			for(int h = 0; h < H; h ++)
				setState(h, false);
			
			F(); // debug
		}
		
		System.out.println();
		System.out.println("ANALYSING..........................");
		
		analyse();
	}
	
	private void initial(){
		Random rand = new Random();
		
		x = new double[H][K][N];
		w = new double[K];
		for(int h = 0; h < H; h ++)
		for(int k = 0; k < K; k ++){
			double s = 0;
			for(int i = 0; i < N; i ++){
				double r= rand.nextDouble();
				x[h][k][i] = r;
				s += r;
			}
			for(int i = 0; i < N; i ++){
				x[h][k][i] /= s;
			}
		}
		w[0] = 0.8 * Lambda;
		for(int k = 1; k < K; k ++){
			w[k] = 0.05 * Lambda;
		}
		/*
		for(int k = 0; k < K; k ++){
			w[k] = Lambda*2*(K-k)/(K*(K+1));
		}*/
	}
	
	private double searchLambda() {// weight
		double[] d = new double[K];
		double[] dw = new double[K];
		double threshold = 1e-20;
		int iteration = 0;
		double norm0 = 1.0;
		
		/////////////////// compute JMatrix ////////////////
		
		double[][] JMatrix = new double[K][K];
		
		for(int h = 0; h < H; h ++)
		for(int k = 0; k < K; k ++){
			for(int k_ = k; k_ < K; k_ ++){
				
				double temp = 0;
				for(int i = 0; i < N; i ++){
					temp += x[h][k][i]*x[h][k_][i];
				}
				
				JMatrix[k][k_] += 2*M*temp;
				
				temp = 0;
				
				for(int i = 0; i < N; i ++){
					for(int j = 0; j < N; j ++){
						temp += x[h][k][i]*x[h][k][j]*x[h][k_][i]*x[h][k_][j];
					}
				}
				
				JMatrix[k][k_] += 2*temp;
			}
		}
		
		
		
		////////////////////////////////////////////////////

		boolean first = true;
		do {
			if (first) {
				first = false;
				
			} else {
				double J = 0;
				
				///// compute J /////////////////////////////
				for(int k = 0; k < K; k ++){
					double temp = 0;
					for(int k_ = 0; k_ < K; k_ ++){
						temp += dw[k_]*getElementfromSymmetricMatrix(JMatrix, k_, k);
					}
					J += temp*dw[k];
				}
				/////////////////////////////////////////////

				double step = dot(dw, d) / J;

				for (int k = 0; k < K; k++) {
					w[k] = w[k] - step * dw[k];
				}
			}

			//////////////////////////
			
			for (int k = 0; k < K; k ++) {
				d[k] = 0;
			}

			for(int h = 0; h < H; h ++){
				
			double[] CommonV = new double[N];
			for(int i = 0; i < N; i ++){
				for(int k_ = 0; k_ < K; k_ ++){
					CommonV[i] += w[k_]*x[h][k_][i];
				}
				CommonV[i] -= e[h][i];
			}
			for (int k = 0; k < K; k ++) {
				double temp = 0;
				for (int i = 0 ; i < N; i ++) {
					temp += CommonV[i]*x[h][k][i];
				}
				d[k] += 2*M*temp;
			}

			for (int k = 0; k < K; k++) {
				for (int i = 0; i < N; i++) {
					for (int j = 0; j < N; j++) {
						double temp = 0;
						for (int k_ = 0; k_ < K; k_++) {
							temp += w[k_] * x[h][k_][i] * x[h][k_][j];//????  change later
						}
						d[k] += 2 * x[h][k][i] * x[h][k][j] * (temp - a[h][i][j]);
					}
				}
			}
			
			}
			
			///////// dw //////////////////////////
			double sum = sum(d);
			for(int k = 0; k < K; k ++){
				dw[k] = d[k] - sum/K;
			}
			///////////////////////////////////////

			norm0 = norm(K, dw);

			if (iteration > MAX_SEARCH_STEP)
				break;

			iteration++;
			
			

		} while (norm0 > threshold);
		
		System.out.println("Lambda:" + iteration);
		return norm0;
	}
	
	private double searchTopic(int h, int k) {
		double[] d = new double[N];
		double[] dx = new double[N];
		double threshold = 1e-20;
		int iteration = 0;
		double norm0 = 1.0;
		
		
		double[][] CommonMatrix = new double[N][N];
		for (int i = 0; i < N; i++) {
			for (int j = i; j < N; j++) {
				CommonMatrix[i][j] -= a[h][i][j];
				for (int k_ = 0; k_ < K; k_++) {
					if (k_ != k) {
						CommonMatrix[i][j] += w[k_] * x[h][k_][i] * x[h][k_][j];
					}
				}
			}
		}
		
		double[] CommonVector = new double[N];
		for(int i = 0; i < N; i ++){
			CommonVector[i] -= e[h][i];
			for (int k_ = 0; k_ < K; k_++) {
				if (k_ != k) {
					CommonVector[i] += w[k_]*x[h][k_][i];
				}
			}
		}
		

		boolean first = true;
		do {
			if (first) {
				first = false;
				
			} else {
				/////////////////// compute JMatrix ////////////////
				
				double[][] JMatrix = new double[N][N];
				
				double common1 = 0;
				for(int j = 0; j < N; j ++){
					common1 += x[h][k][j]*x[h][k][j];
				}
				common1 *= (4*w[k]*w[k]);
				
				double common2 = 2*M*w[k]*w[k];
				
				//double common3 = 2*M;
				
				for (int i = 0; i < N; i++) {
					for (int j = i; j < N; j++) {

						JMatrix[i][j] += 2 * w[k] * x[h][k][i] * x[h][k][j];
						JMatrix[i][j] += getElementfromSymmetricMatrix(
								CommonMatrix, i, j);
						JMatrix[i][j] *= 4 * w[k];
						//JMatrix[i][j] += common3;
						
						if(i == j){
							JMatrix[i][i] += (common1 + common2);
						}

					}
				}
				
				
				
				////////////////////////////////////////////////////
				double J = 0;
				
				///// compute J /////////////////////////////
				for(int i = 0; i < N; i ++){
					double temp = 0;
					for(int j = 0; j < N; j ++){
						temp += dx[j]*getElementfromSymmetricMatrix(JMatrix, j, i);
					}
					J += temp*dx[i];
				}
				/////////////////////////////////////////////

				double step = dot(dx, d) / J;
				
				/*
				double stepMin = -1e300;
				double stepMax = 1e300;
				for(int i = 0; i < N; i ++){
					if(dx[i] > 0){
						double temp = x[h][k][i]/dx[i];
						if(temp < stepMax){
							stepMax = temp;
						}
					}
					if(dx[i] < 0){
						double temp = x[h][k][i]/dx[i];
						if(temp > stepMin){
							stepMin = temp;
						}
					}
				}
				
				//if(step < stepMin) step = stepMin;
				//if(step > stepMax) step = stepMax;
				*/
				
				for (int i = 0; i < N; i++) {
					x[h][k][i] = x[h][k][i] - step * dx[i];
				}
			}

			//////////////compute d ////////////
			/*
			double Common = 0;
			for(int i = 0; i < N; i ++){
				Common += x[k][i];
			}
			Common -= 1;
			Common *= 2*M;*/
			
			for(int i = 0; i < N; i ++){
				d[i] = 0;
				for(int j = 0; j < N; j ++){
					d[i] += (w[k]*x[h][k][i]*x[h][k][j] + getElementfromSymmetricMatrix(CommonMatrix, i, j))*x[h][k][j];
				}
				d[i] *= 4*w[k];
				//d[i] += Common;
				d[i] += 2*M*w[k]*(w[k]*x[h][k][i] + CommonVector[i]);
			}

			//////////////////////////////////////
			
			///////// dx //////////////////////////
			double sum = sum(d);
			for(int i = 0; i < N; i ++){
				dx[i] = d[i] - sum/N;
			}
			///////////////////////////////////////

			norm0 = norm(N, dx);

			if (iteration > MAX_SEARCH_STEP)
				break;

			iteration++;
						

		} while (norm0 > threshold);
		
		System.out.println("Topic "+ h + " " + iteration);
		return norm0;
	}
	
	private double getElementfromSymmetricMatrix(double[][] m, int i, int j){
		if(i <= j) return m[i][j];
		return m[j][i];
	}
	
	private double norm(int n, double[] d) {
		double s = 0;
		for (int i = 0; i < n; i++) {
			s += d[i] * d[i];
		}
		return s;
	}
	
	private double dot(double[] v1, double[] v2){
		double ret = 0;
		int n = v1.length;
		for(int i = 0; i < n; i ++){
			ret += v1[i]*v2[i];
		}
		return ret;
	}
	
	private double sum(double[] v){
		double ret = 0;
		int n = v.length;
		for(int i = 0; i < n; i ++){
			ret += v[i];
		}
		return ret;
	}
	
	class Handler implements Runnable {
		int h = -1;

		Handler(int h) {
			this.h = h;
		}

		public void run() {
			for(int k = 0; k < K; k ++){
				searchTopic(h, k);
			}
			setState(h, true);
		}
	}
	
	synchronized private void setState(int h, boolean finished){
		states[h] = finished;
	}
	
	synchronized private boolean getState(){
		
		for(int h = 0; h < H; h ++){
			if(states[h] == false)
				return false;
		}
		return true;
		
	}
	
	private void load(String dir, char c){
		
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
			
			////// for actives
			in = new BufferedReader(new FileReader(dir + "/" + "actives.txt"));
			while((line = in.readLine()) != null){
				int term = Integer.parseInt(line);
				actives.add(term);
			}
			in.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	private double F(){
		double ret = 0;
		
		double F1 = 0;
		for(int h = 0; h < H; h ++)
		for(int i = 0; i < N; i ++){
			for(int j = 0; j < N; j ++){
				double temp = 0;
				for(int k = 0; k < K; k ++){
					temp += w[k]*x[h][k][i]*x[h][k][j];
				}
				temp -= a[h][i][j];
				F1 += temp*temp;
			}
		}
		System.out.println("F1 : " + F1);
		
		double F2 = 0;
		for(int h = 0; h < H; h ++)
		for(int k = 0; k < K; k ++){
			double temp = 0;
			for(int i = 0; i < N; i ++){
				temp += x[h][k][i];
			}
			temp -= 1;
			F2 += temp*temp;
		}
		System.out.println("F2 : " + F2);
		
		double F3 = 0;
		for(int k = 0; k < K; k ++){
			F3 += w[k];
		}
		F3 -= Lambda;
		F3 = F3*F3;
		System.out.println("F3 : " + F3);
		
		double F4 = 0;
		for(int h = 0; h < H; h ++)
		for(int i = 0; i < N; i ++){
			double temp = 0;
			for(int k = 0; k < K; k ++){
				temp += w[k]*x[h][k][i];
			}
			temp -= e[h][i];
			F4 += temp*temp;
		}

		System.out.println("F4 : " + F4);
		
		ret = F1 + M * F4;
		System.out.println("F : " + ret);
		return ret;
	}
	
	private void analyse(){
		final int TopN = 15;

		for (int k = 0; k < K; k++) {
			PriorityQueue<ValueIdPair> queue = new PriorityQueue<ValueIdPair>(
					TopN, new Comparator<ValueIdPair>() {

						@Override
						public int compare(ValueIdPair arg0, ValueIdPair arg1) {
							if (arg0.v > arg1.v)
								return 1;
							if (arg0.v < arg1.v)
								return -1;
							return 0;
						}

					});

			for (int id : actives) {
				double min = 1e10;
				for (int h = 0; h < H; h++) {
					double v = x[h][k][HashFamily.hash(h, id)];
					if(v < 0) v = 0;
					if (v < min)
						min = v;
				}
				if (queue.size() < TopN) {
					queue.offer(new ValueIdPair(min, id));
				} else {
					ValueIdPair pair = queue.peek();
					if(pair.v < min){
						queue.poll();
						queue.offer(new ValueIdPair(min, id));
					}
				}
			}

			double sumW = sum(w);
			System.out.println("Topic " + k + " ---------" + "   " + w[k]/sumW);
			for (ValueIdPair pair : queue) {
				System.out.println(getWord(pair.id) + "\t" + pair.v);
			}
			System.out.println("------------------------------------------");

		}
	}
	
	private class ValueIdPair{
		public double v = 0;
		public int id = 0;
		public ValueIdPair(double v, int id){
			this.v = v;
			this.id = id;
		}
	}
	
	
	static private Connection conn = null;
	
	static {
		try {
			conn = DriverManager
					.getConnection("jdbc:mysql://10.4.8.16/tweetstream?"
							+ "user=root&password=123583");

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

			conn = null;

		}
	}
	
	private String getWord(int id) {
		String word = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {

			stmt = conn.createStatement();

			if (stmt.execute("select  term from idterm where id =" + id)) {
				rs = stmt.getResultSet();
				while (rs.next()) {

					word = rs.getString("term");

				}
			}

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());

		} finally {
			// it is a good idea to release
			// resources in a finally{} block
			// in reverse-order of their creation
			// if they are no-longer needed
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				} // ignore
				rs = null;
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				} // ignore
				stmt = null;
			}
		}
		return word;
	}
	
}

