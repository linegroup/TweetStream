package linegroup3.tweetstream.em;

import java.util.Map;

public interface UnitProcessor {
	public void processUnit(Map<String, Integer> unit); // unit is a map : from word to count
}
