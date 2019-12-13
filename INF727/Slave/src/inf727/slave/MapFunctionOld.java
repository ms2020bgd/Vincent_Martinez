package inf727.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * This class is used to calculate the Map Function
 * 
 * @author martinez
 *
 */

public class MapFunctionOld {

	String directory;
	HashMap<String, ArrayList<Integer>> maps = new HashMap<String, ArrayList<Integer>>();

	public MapFunctionOld(String directory) {
		this.directory = directory;
	}

	public void processedMap() {
		// Make a word count depending of the files in the directory
		File f = new File(directory);

		Arrays.stream(f.listFiles()).filter(l -> l.isFile() && !l.getName().endsWith("jar")).forEach(it -> {
			try {
				wordCount(it);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

		writeOnDisk();

	}

	private void wordCount(File f) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(f));
		String str = null;
		while ((str = reader.readLine()) != null) {
			StringTokenizer token = new StringTokenizer(str, " \t\n\r\f,;.:!?\"'");
			while (token.hasMoreTokens()) {
				String word = token.nextToken();
				if (!word.isEmpty()) {
					ArrayList<Integer> count = maps.get(word);
					if (count == null) {
						ArrayList<Integer> cts = new ArrayList<Integer>();
						cts.add(1);
						maps.put(word, cts);
					} else {
						count.add(1);
					}
				}
			}
			// In order to have a msg in my program, I write the line
			System.out.println(str);
		}
		reader.close();

	}

	private void writeOnDisk() {
		// We will write on the disk the result and prepare the shuffle
		// Not a good way to use the hash of the word...

		File f = new File(directory + "/maps");
		f.mkdir(); // create the directory
		// Write the files

		maps.entrySet().stream().forEach(it -> {
			try {
				writeResultFile(f.getAbsolutePath(), it.getKey(), it.getValue());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}

	private void writeResultFile(String result, String word, ArrayList<Integer> values) throws IOException {

		long hashcode = Integer.toUnsignedLong(InetAddress.getLocalHost().getHostName().hashCode());

		File f = new File(result + "/" + Integer.toUnsignedLong(word.hashCode()) + "_" + hashcode);

		FileWriter fileWriter = new FileWriter(f);

		for (int value : values) {
			fileWriter.write(word + " " + value + "\n");
		}

		fileWriter.close();
	}

}
