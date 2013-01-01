package linegroup3.tweetstream.preparedata;


public class HashFamily {

	/*
	private static long[] seeds = { 31, 131, 1313, 13131, 131313 };

	public static int hash(int fun, int i) { // return 0~199 fun 0~4

		long ret = BKDRHash(seeds[fun], "" + i) % 200;
		return (int)ret;
	}

	private static long BKDRHash(long seed, String str) {
		
		long hash = 0;

		for (int i = 0; i < str.length(); i++) {
			hash = (hash * seed) + str.charAt(i);
		}

		return hash;
	}
	/* End Of BKDR Hash Function */
	
	
	private static GeneralHashFunctionLibrary hashBase = new GeneralHashFunctionLibrary();
	
	private static int hashCode(int fun, String str) { // return 0~199 fun 0~4
		long ret = 0;
		switch(fun){
		case 0:{
			ret = hashBase.BKDRHash(str);
		}break;
		case 1:{
			ret = hashBase.APHash(str);
		}break;
		case 2:{
			ret = hashBase.DJBHash(str);
		}break;
		case 3:{
			ret = hashBase.JSHash(str);
		}break;
		case 4:{
			ret = hashBase.RSHash(str);
		}break;
		}
		
		if(ret < 0)	ret += 200;
		return (int)ret;
	}
	
	
	
	
	////// USING UNIVERSAL HASHING
	private static long[] a = {179213003, 50325253, 81443410, 30332510, 84570308};
	private static long[] b = {50216209, 102147962, 171271222, 55721943, 140845500};
	private static long P = 179401571;

	/*
	static{
		Random rand = new Random();
		for(int i = 0; i < 5; i ++){
			a[i] = 0;
			while(a[i] == 0)
				a[i] = rand.nextInt((int)P);
			
			b[i] = rand.nextInt((int)P);;
			
			System.out.println("" + a[i] + "\t" + b[i]);
		}
	}*/
	
	public static int hash(int fun, int i) { // return 0~199 fun 0~4
		//long ret = ((b[fun] + i*a[fun]) % P ) % 200 ;
		long ret = (b[fun] + i*a[fun]);		
		ret = (ret % P) % 200;
		return (int)ret;
		
	}
	
	public static int hash(int fun, String str) { // return 0~199 fun 0~4
		int i = Math.abs(hashCode(fun, str));
		
		//long ret = ((b[fun] + i*a[fun]) % P ) % 200 ;
		long ret = (b[fun] + i*a[fun]);		
		ret = (ret % P) % 200;
		return (int)ret;
		
	}

	
	
	

}
