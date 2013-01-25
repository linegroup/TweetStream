package linegroup3.tweetstream.onlinelda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Test {
	
	public static void test() throws IOException{
		OnlineLDA model = new OnlineLDA();
		
		model.beforeLoad();
		
		File[] files = new File("C:/Users/wei.xie.2012/Dropbox/temp/docs").listFiles();
		
		for (int i = 0; i < files.length; i++) {
			if (files[i].getName().startsWith(".")) {
				continue;
			}
			BufferedReader reader = new BufferedReader(new FileReader(files[i]));
			String line = null;
			List<String> txt = new LinkedList<String>();
			while ((line = reader.readLine()) != null) {
				txt.add(line);
			}
			reader.close();
			
			model.loadDoc(txt);

		}
		
		model.train(1000);
		
		System.out.println("---------------------------------------------------------------");
		
		model.beforeLoad();
		
		files = new File("C:/Users/wei.xie.2012/Dropbox/temp/docs2").listFiles();
		
		for (int i = 0; i < files.length; i++) {
			if (files[i].getName().startsWith(".")) {
				continue;
			}
			BufferedReader reader = new BufferedReader(new FileReader(files[i]));
			String line = null;
			List<String> txt = new LinkedList<String>();
			while ((line = reader.readLine()) != null) {
				txt.add(line);
			}
			reader.close();
			
			model.loadDoc(txt);

		}
		
		model.train(1000);
	}

}
