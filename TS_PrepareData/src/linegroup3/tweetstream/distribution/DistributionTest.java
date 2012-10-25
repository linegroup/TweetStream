package linegroup3.tweetstream.distribution;

import java.util.Random;

public class DistributionTest {
	private static int K = 5;  // number of words
	private static int N = 2; // short length
	private static double[] p = new double[K];
	private static Random rand = new Random();
	
	static{
		/*
		double s = 0;
		for(int k = 0; k < K; k ++){
			double t = Math.exp(rand.nextGaussian()/2);
			p[k] = t;
			s += t;
		}
		for(int k = 0; k < K; k ++){
			p[k] /= s;
		}*/
		
		for(int k = 0; k < K; k ++){
			p[k] = 1.0 / K;
		}
	}
	
	private static int drawWord(){
		
		double r = rand.nextDouble();
		double s = 0;
		for(int k = 0; k < K; k ++){
			s += p[k];
			if(r <= s)	return k;
		}
		
		return K - 1;
	}
	
	public static void test(int testTime){
		double[] d0 = new double[K];
		double[] d1 = new double[K];
		for(int n = 0; n < testTime; n ++){
			
			int[] counts = new int[K];
			for(int i = 0; i < N; i ++){
				int word = drawWord();
				
				counts[word] ++;
			}
			
			for(int k = 0; k < K; k ++){

				double d = (double)counts[k] / N;
				d = d * d;
				
				d0[k] += d;
				
				double d_ = (double)counts[k];
				d_ = d_*(d_-1);
				d_ /= N*(N-1);
				
				d1[k] += d_;				
			}
		}
		
		for(int k = 0; k < K; k ++){
			System.out.println((d0[k]/testTime - p[k]*p[k]) + "\t" + (d1[k]/testTime - p[k]*p[k]));
		}
		for(int k = 0; k < K; k ++){
			System.out.println(Math.abs(d1[k]/testTime - p[k]*p[k]) / Math.abs(d0[k]/testTime - p[k]*p[k]));
		}
	}
	
	public static void test2(int testTime){
		double[] d0 = new double[K];
		double[] d1 = new double[K];
		for(int n = 0; n < testTime; n ++){
			
			int[] counts = new int[K];
			for(int i = 0; i < N; i ++){
				int word = drawWord();
				
				counts[word] ++;
			}
			
			for(int k = 0; k < K; k ++){

				double d = (double)counts[k] / N;
				d = d * counts[0] / N;
				
				d0[k] += d;
				
				double d_ = (double)counts[k] / N ;
				d_ = d_* counts[0] / (N - 1);
				
				d1[k] += d_;				
			}
		}
		
		for(int k = 1; k < K; k ++){
			System.out.println((d0[k]/testTime - p[k]*p[0]) + "\t" + (d1[k]/testTime - p[k]*p[0]));
		}
		for(int k = 1; k < K; k ++){
			System.out.println(Math.abs(d1[k]/testTime - p[k]*p[0]) / Math.abs(d0[k]/testTime - p[k]*p[0]));
		}
	}
	
}
