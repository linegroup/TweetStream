package linegroup3.tweetstream.inference;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Mixture_model {
	private double[][] a;
	private double[][] topics = null;
	private double[] lambda = null;
	private double[] e = null;
	private double Lambda = 0;

	private double[][] x = null; // guess for topics
	private double[] w = null; // guess for lambda, w means weight

	private int MAX_SEARCH_STEP = 10;
	private double M = 1e2;

	private int N = 0;
	private int K = 0;

	public void doJob(int n, int k, double[][] initial_x, double[] initial_lda) {
		N = n;
		K = k;
		load();

		x = initial_x;
		w = initial_lda;
		//x = topics;
		//w = lambda;
		
		search();
		
		System.out.println("OK!");

	}
	
	private double search(){
		double norm = 1.0;
		double threshold = 1e-5;
		int iteration = 0;
		while(norm > threshold && iteration < 100){
			searchLambda();
			for(int k = 0; k < K; k ++){
				searchTopic(k);
			}
			F();
			iteration ++;
			
			System.out.println("-------search : " + iteration);
			for(int k = 0; k < K; k ++)
				System.out.print("" + w[k] + ",");
			System.out.println();
			
			MAX_SEARCH_STEP ++;
		}
		
		
		
		return norm;
		
	}

	private double searchLambda() {// weight
		double[] d = new double[K];
		double threshold = 1e-5;
		int iteration = 0;
		double norm0 = 1.0;
		
		/////////////////// compute JMatrix ////////////////
		
		double[][] JMatrix = new double[K][K];
		/*
		//////// for diag
		for(int k = 0; k < K; k ++){
			JMatrix[k][k] = 2*M;
			
			double temp = 0;
			for(int i = 0; i < N; i ++){
				temp += x[k][i]*x[k][i];
			}
			
			JMatrix[k][k] += 2*M*temp;
			
			temp = 0;
			
			for(int i = 0; i < N; i ++){
				for(int j = 0; j < N; j ++){
					double xixj = x[k][i]*x[k][j];
					temp += xixj*xixj;
				}
			}
			
			JMatrix[k][k] += 2*temp;
		}*/
		
		for(int k = 0; k < K; k ++){
			for(int k_ = k; k_ < K; k_ ++){
				JMatrix[k][k_] = 2*M;
				
				double temp = 0;
				for(int i = 0; i < N; i ++){
					temp += x[k][i]*x[k_][i];
				}
				
				JMatrix[k][k_] += 2*M*temp;
				
				temp = 0;
				
				for(int i = 0; i < N; i ++){
					for(int j = 0; j < N; j ++){
						temp += x[k][i]*x[k][j]*x[k_][i]*x[k_][j];
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
						temp += d[k_]*getElementfromSymmetricMatrix(JMatrix, k_, k);
					}
					J += temp*d[k];
				}
				/////////////////////////////////////////////

				double step = norm0 / J;

				for (int k = 0; k < K; k++) {
					w[k] = w[k] - step * d[k];
				}
			}

			//////////////////////////
			
			for (int k = 0; k < K; k++) {
				d[k] = 0;
			}

			double Common = 0;
			for (int k = 0; k < K; k++) {
				Common += w[k];
			}
			Common = 2 * (Common - Lambda) * M;
			
			double[] CommonV = new double[N];
			for(int i = 0; i < N; i ++){
				for(int k_ = 0; k_ < K; k_ ++){
					CommonV[i] += w[k_]*x[k_][i];
				}
				CommonV[i] -= e[i];
			}
			for (int k = 0; k < K; k ++) {
				double temp = 0;
				for (int i = 0 ; i < N; i ++) {
					temp += CommonV[i]*x[k][i];
				}
				d[k] += 2*M*temp;
			}

			for (int k = 0; k < K; k++) {
				for (int i = 0; i < N; i++) {
					for (int j = 0; j < N; j++) {
						double temp = 0;
						for (int k_ = 0; k_ < K; k_++) {
							temp += w[k_] * x[k_][i] * x[k_][j];
						}
						d[k] += 2 * x[k][i] * x[k][j] * (temp - a[i][j]);
					}
				}
				d[k] += Common;
			}

			norm0 = norm(K, d);

			if (iteration > MAX_SEARCH_STEP)
				break;

			iteration++;
			
			

		} while (norm0 > threshold);
		
		System.out.println(iteration);
		return norm0;
	}

	private double searchTopic(int k) {
		double[] d = new double[N];
		double threshold = 1e-5;
		int iteration = 0;
		double norm0 = 1.0;
		
		
		double[][] CommonMatrix = new double[N][N];
		for (int i = 0; i < N; i++) {
			for (int j = i; j < N; j++) {
				CommonMatrix[i][j] -= a[i][j];
				for (int k_ = 0; k_ < K; k_++) {
					if (k_ != k) {
						CommonMatrix[i][j] += w[k_] * x[k_][i] * x[k_][j];
					}
				}
			}
		}
		
		double[] CommonVector = new double[N];
		for(int i = 0; i < N; i ++){
			CommonVector[i] -= e[i];
			for (int k_ = 0; k_ < K; k_++) {
				if (k_ != k) {
					CommonVector[i] += w[k_]*x[k_][i];
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
					common1 += x[k][j]*x[k][j];
				}
				common1 *= (4*w[k]*w[k]);
				
				double common2 = 2*M*w[k]*w[k];
				
				double common3 = 2*M;
				
				for (int i = 0; i < N; i++) {
					for (int j = i; j < N; j++) {

						JMatrix[i][j] += 2 * w[k] * x[k][i] * x[k][j];
						JMatrix[i][j] += getElementfromSymmetricMatrix(
								CommonMatrix, i, j);
						JMatrix[i][j] *= 4 * w[k];
						JMatrix[i][j] += common3;
						
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
						temp += d[j]*getElementfromSymmetricMatrix(JMatrix, j, i);
					}
					J += temp*d[i];
				}
				/////////////////////////////////////////////

				double step = norm0 / J;
				
				double stepMin = -1e300;
				double stepMax = 1e300;
				for(int i = 0; i < N; i ++){
					if(d[i] > 0){
						double temp = x[k][i]/d[i];
						if(temp < stepMax){
							stepMax = temp;
						}
					}
					if(d[i] < 0){
						double temp = x[k][i]/d[i];
						if(temp > stepMin){
							stepMin = temp;
						}
					}
				}
				
				if(step < stepMin) step = stepMin;
				if(step > stepMax) step = stepMax;
				
				for (int i = 0; i < N; i++) {
					x[k][i] = x[k][i] - step * d[i];
				}
			}

			//////////////compute d ////////////
			double Common = 0;
			for(int i = 0; i < N; i ++){
				Common += x[k][i];
			}
			Common -= 1;
			Common *= 2*M;
			
			for(int i = 0; i < N; i ++){
				d[i] = 0;
				for(int j = 0; j < N; j ++){
					d[i] += (w[k]*x[k][i]*x[k][j] + getElementfromSymmetricMatrix(CommonMatrix, i, j))*x[k][j];
				}
				d[i] *= 4*w[k];
				d[i] += Common;
				d[i] += 2*M*w[k]*(w[k]*x[k][i] + CommonVector[i]);
			}

			//////////////////////////////////////

			norm0 = norm(N, d);

			if (iteration > MAX_SEARCH_STEP)
				break;

			iteration++;
						

		} while (norm0 > threshold);
		
		System.out.println(iteration);
		return norm0;
	}

	private void loadA() {
		a = new double[N][];
		try {
			BufferedReader in = new BufferedReader(new FileReader("a.txt"));
			String line = null;
			int i = 0;
			while ((line = in.readLine()) != null) {
				a[i] = new double[N];
				String[] terms = line.split("\\s+");
				for (int j = 0; j < N; j++) {
					a[i][j] = Double.parseDouble(terms[j + 1]);
				}
				i++;
			}
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void loadTopics() {
		topics = new double[K][];
		try {
			BufferedReader in = new BufferedReader(new FileReader("topics.txt"));
			String line = null;
			int i = 0;
			while ((line = in.readLine()) != null) {
				topics[i] = new double[N];
				String[] terms = line.split("\\s+");
				for (int j = 0; j < N; j++) {
					topics[i][j] = Double.parseDouble(terms[j + 1]);
				}
				i++;
			}
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void loadLambda() {
		Lambda = 0;
		lambda = new double[K];
		try {
			BufferedReader in = new BufferedReader(new FileReader("lambda.txt"));
			String line = null;
			int i = 0;
			while ((line = in.readLine()) != null) {
				String[] terms = line.split("\\s+");
				lambda[i] = Double.parseDouble(terms[1]);
				Lambda += lambda[i];
				i++;
			}
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private void loadE() {
		e = new double[N];
		try {
			BufferedReader in = new BufferedReader(new FileReader("e.txt"));
			String line = null;
			while ((line = in.readLine()) != null) {
				String[] terms = line.split("\\s+");
				for (int j = 0; j < N; j++) {
					e[j] = Double.parseDouble(terms[j + 1]);
				}
				break;
			}
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private double norm(int n, double[] d) {
		double s = 0;
		for (int i = 0; i < n; i++) {
			s += d[i] * d[i];
		}
		return s;
	}

	private void load() {
		loadE();
		loadA();
		loadTopics();
		loadLambda();
	}
	
	private double getElementfromSymmetricMatrix(double[][] m, int i, int j){
		if(i <= j) return m[i][j];
		return m[j][i];
	}
	
	private double F(){
		double ret = 0;
		
		double F1 = 0;
		for(int i = 0; i < N; i ++){
			for(int j = 0; j < N; j ++){
				double temp = 0;
				for(int k = 0; k < K; k ++){
					temp += w[k]*x[k][i]*x[k][j];
				}
				temp -= a[i][j];
				F1 += temp*temp;
			}
		}
		System.out.println("F1 : " + F1);
		
		double F2 = 0;
		for(int k = 0; k < K; k ++){
			double temp = 0;
			for(int i = 0; i < N; i ++){
				temp += x[k][i];
			}
			temp -= 1;
			F2 += temp*temp;
		}
		F2 *= M;
		System.out.println("F2 : " + F2);
		
		double F3 = 0;
		for(int k = 0; k < K; k ++){
			F3 += w[k];
		}
		F3 -= Lambda;
		F3 = F3*F3;
		F3 *= M;
		System.out.println("F3 : " + F3);
		
		double F4 = 0;
		for(int i = 0; i < N; i ++){
			double temp = 0;
			for(int k = 0; k < K; k ++){
				temp += w[k]*x[k][i];
			}
			temp -= e[i];
			F4 += temp*temp;
		}
		F4 *= M;
		System.out.println("F4 : " + F4);
		
		
		return ret;
	}
}
