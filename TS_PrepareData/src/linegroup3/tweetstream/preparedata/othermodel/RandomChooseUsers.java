package linegroup3.tweetstream.preparedata.othermodel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Random;

public class RandomChooseUsers {
	final static double R = 0.4;
	final static Random rand = new Random();

	public static void choose() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					"D:/othermodel/users.txt"));
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					"D:/othermodel/filelist.txt"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if(rand.nextDouble() <= R){
					writer.write(line);
					writer.write("\n");
				}
			}
			writer.close();
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
