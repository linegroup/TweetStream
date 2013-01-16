package linegroup3.tweetstream.burstdetect;

import java.util.Arrays;

public class TwoStateMachine {

	private double lamda0 = 0;

	private double lamda1 = 0;

	private double p0 = 0;

	private double p1 = 0;

	private int N = 0;

	private int[] count = null;

	private double[][] C = null;

	private int[][] choice = null;

	public TwoStateMachine load(int[] data) {

		N = data.length;

		count = new int[N];

		for (int i = 0; i < N; i++) {

			count[i] = data[i];

		}

		C = new double[N][2];

		choice = new int[N - 1][2];
		
		return this;

	}

	private void initialParameters() {

		lamda0 = mid();
		
		if(lamda0 == 0){
			double v = average();
			lamda0 = Math.min(v, 1);
		}

		lamda1 = 3 * lamda0;

		p0 = 0.9;

		p1 = 0.6;

	}
	
	private double average(){
		double s = 0.0;
		for(int i = 0; i < N; i ++){
			s += count[i];
		}
		return s / N;
	}
	
	private double mid(){
		int[] array = new int[N];
		for(int i = 0; i < N; i ++){
			array[i] = count[i];
		}
		Arrays.sort(array);
		if(N % 2 == 0)
			return (array[(N/2) - 1] + array[N/2]) / 2.0;
		return array[(N - 1)/2];
	}

	public int[] infer() {

		initialParameters();

		C[0][0] = logProb(count[0], lamda0);

		C[0][1] = logProb(count[0], lamda1);

		for (int i = 1; i < N; i++) {

			double c0 = C[i - 1][0] + Math.log(p0);

			double c1 = C[i - 1][1] + Math.log(1 - p1);

			double lp = logProb(count[i], lamda0);

			if (c0 > c1) {

				C[i][0] = c0 + lp;

				choice[i - 1][0] = 0;

			} else {

				C[i][0] = c1 + lp;

				choice[i - 1][0] = 1;

			}

			c0 = C[i - 1][0] + Math.log(1 - p0);

			c1 = C[i - 1][1] + Math.log(p1);

			lp = logProb(count[i], lamda1);

			if (c0 > c1) {

				C[i][1] = c0 + lp;

				choice[i - 1][1] = 0;

			} else {

				C[i][1] = c1 + lp;

				choice[i - 1][1] = 1;

			}

		}

		int[] state = new int[N];

		if (C[N - 1][0] > C[N - 1][1]) {

			state[N - 1] = 0;

		} else {

			state[N - 1] = 1;

		}

		for (int i = N - 2; i >= 0; i--) {

			state[i] = choice[i][state[i + 1]];

		}

		return state;

	}

	private double logProb(int n, double lamda) {

		double ret = 0.0;

		ret += n * Math.log(lamda);

		ret -= lamda;

		for (int i = 1; i <= n; i++) {

			ret -= Math.log(i);

		}

		return ret;

	}

}