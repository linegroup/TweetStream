package linegroup3.tweetstream.onlinelda;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;



public class OnlineLDA {
	
	public OnlineLDA(double alpha, double beta, int nTopic, int nWord){
		this.alpha = alpha;
		this.beta = beta;
		this.nTopic = nTopic;
		this.nWord = nWord;
		
		System.out.println("alpha\t" + alpha);
		System.out.println("beta\t" + beta);
		System.out.println("nTopic\t" + nTopic);
		System.out.println("nWord\t" + nWord);
	}
	
	public OnlineLDA(){
		
	}
	
	
	
	public void train(int nIter) throws IOException{
		initialTable();
		
		
		for(int i = 0; i < nIter; i ++){
			gibbsSample();
		}
		
		print();
		
		System.out.println("WZtable size is" + WZtable.size());
		
		analyse();
	}
	
	/*private void print(){
		for(Map.Entry<String, int[]> entry : WZtable.entrySet()){
			double[] p = new double[nTopic];
			for(int z = 0; z < nTopic; z ++){
				double c = entry.getValue()[z];
				p[z] = c/Wcounts[z];
			}
			if(max(p) >= 0.01){
				System.out.print(entry.getKey() + "\t");
				for(int z = 0; z < nTopic; z ++){
					System.out.print(p[z] + "\t");
				}
				System.out.println();
			}
			
		}
	}*/
	
	private void print(){
		for(int z = 0; z < nTopic; z ++){
			System.out.print("topic" + z + ":\t");
			
			for(Map.Entry<String, int[]> entry : WZtable.entrySet()){
				double p = entry.getValue()[z];
				p /= Wcounts[z];
				if(p >= 0.01){
					p *= 100;
					System.out.print(entry.getKey() + " ");
					System.out.print((int)p + "%,");
				}
			}
			
			System.out.println();
		}
	}
	
	/*
	private double max(double[] v){
		double ret = 0;
		for(int i = 0; i < v.length; i ++){
			if(ret < v[i]){
				ret = v[i];
			}
		}
		return ret;
	}*/
	
	public void beforeLoad(){
		docs = new LinkedList<Doc>();
		docId = 0;
	}
	

	
	public void loadDoc(List<String> txt){
		List<Token> tokens = new LinkedList<Token>();
		Doc doc = new Doc();
		doc.docId = docId;
		doc.tokens = tokens;
		for (String str : txt) {
			doc.tokens.add(new Token(-1, str));
		}
		docs.add(doc);
		docId ++;
	}
	
	
	private void initialTable() throws IOException{
		
		int nDoc = docs.size();
		
		preWZtable = WZtable;
		preWcounts = Wcounts;
		
		DZtable = new int[nDoc][nTopic];
		Wcounts = new int[nTopic];
		WZtable = new HashMap<String, int[]>(nWord);
		
		randomSample();
	}
	
	private int chooseZ(int id, String word){
		double r = rand.nextDouble();
		double[] p = new double[nTopic];
		for(int i = 0; i < nTopic; i ++){
			int pC = (preWcounts == null? 0 : preWcounts[i]);
			if(i == 0){			
				p[i] = (DZtable[id][i] + alpha)*(getCount(WZtable, word, i) + getCount(preWZtable, word, i) + beta)/(Wcounts[i] + pC + nWord*beta);
			}else{
				p[i] = p[i - 1] + (DZtable[id][i] + alpha)*(getCount(WZtable, word, i) + getCount(preWZtable, word, i) + beta)/(Wcounts[i] + pC + nWord*beta);
			}
		}
		for(int i = 0; i < nTopic; i ++){
			if(r < p[i]/p[nTopic - 1])
				return i;
		}
		return -1;
	}
	
	private void gibbsSample(){
		
		for(Doc doc : docs){
			
			int id = doc.docId;
			
			for(Token token : doc.tokens){
				int z = token.z;
				String word = token.v;
				
				int[] topics = WZtable.get(word);
				
				DZtable[id][z] --;
				topics[z] --;
				Wcounts[z] --;
				
				z = chooseZ(id, word);
				token.z = z;
				
				DZtable[id][z] ++;
				topics[z] ++;
				Wcounts[z] ++;
				
				

			}
			
		}
	}
	
	private void randomSample() {
		
		

		for (Doc doc : docs) {
			
			int id = doc.docId;
			
			for(Token token : doc.tokens){
				int z = randomTopic();
				token.z = z;
				
				DZtable[id][z] ++;
				
				int[] topics = WZtable.get(token.v);
				if(topics == null){
					topics = new int[nTopic];
					WZtable.put(token.v, topics);
 				}
				
				topics[z] ++;
				
				Wcounts[z] ++;
				
			}
			
		}
		
	}
	
	private void analyse(){
		double[] tracks = new double[nTopic];
		for (Doc doc : docs) {
			
			int[] t = new int[nTopic];
			
			for(Token token : doc.tokens){
				int z = token.z;
				t[z] ++;
			}
			
			double[] n = normalize(t);
			
			for(int i = 0; i < nTopic; i ++){
				tracks[i] += n[i];
			}	
		}
		
		StringBuilder sb = new StringBuilder();
		for(double track : tracks){
			sb.append(track + "\t");
		}
		sb.append("\n");
		
		System.out.println(sb.toString());
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("./tracks.txt", true));
			out.write(sb.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private double[] normalize(int v[]){
		double s = sum(v);
		double[] ret = new double[v.length];
		for(int i = 0; i < v.length; i ++){
			ret[i] = v[i] / s;
		}
		return ret;
	}
	
	private int sum(int v[]){
		int s = 0;
		for(int e : v){
			s += e;
		}
		return s;
	}
	
	private int randomTopic() {
		return rand.nextInt(nTopic);
	}
	
	
	private double alpha = 1.0;
	private double beta = 0.01;
	private int nTopic = 3;
	
	private int nWord = 26;  //  roughly the total number of different words
	
	
	private Random rand = new Random();
	
	
	private int[][] DZtable = null;
	private Map<String, int[]> preWZtable = null;
	private int[] preWcounts = null;
	private Map<String, int[]> WZtable = null;
	private int[] Wcounts = null;
	
	private int getCount(Map<String, int[]> WZtable, String word, int z){
		if(WZtable == null)	return 0;
		int[] topics = WZtable.get(word);
		if(topics == null)	return 0;
		
		return topics[z];
	}

	
	private List<Doc> docs = null;
	
		

	private static int docId = 0;


}
