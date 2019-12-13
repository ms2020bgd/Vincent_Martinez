package inf727.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This class is used to calculate the Map Function The result are stored in
 * files, depending of the hash count of the key
 * 
 * @author martinez
 *
 */

public class MapFunction {

	String directory;
	Map<Integer, FileWriter> writers = new HashMap<Integer, FileWriter>();
	int machineCount;

	// HashMap<String, ArrayList<Integer>> maps = new HashMap<String,
	// ArrayList<Integer>>();

	public MapFunction(String directory, int machineCount) {
		this.directory = directory;
		this.machineCount = machineCount;
	}

	public void processedMap() {
		// Make a word count depending of the files in the directory
		File f = new File(directory);

		File dir = new File(directory + "/maps");
		dir.mkdir();

		Arrays.stream(f.listFiles()).filter(l -> l.isFile() && !l.getName().endsWith("jar")).forEach(it -> {
			try {
				wordCount(it);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

		// On ferme les writers

		for (FileWriter writer : writers.values())
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

	/**
	 * Word count in the file f. For each, put a "one" in the file, depends in the
	 * right result file
	 * 
	 * @param f
	 * @throws IOException
	 */
	private void wordCount(File f) throws IOException {
		long hashcode = Integer.toUnsignedLong(InetAddress.getLocalHost().getHostName().hashCode());

		BufferedReader reader = new BufferedReader(new FileReader(f));
		String str = null;
		while ((str = reader.readLine()) != null) {
			StringTokenizer token = new StringTokenizer(str, " \t\n\r\f,;.:!?\"'");
			while (token.hasMoreTokens()) {
				String word = token.nextToken();
				if (!word.isEmpty()) {

					// Compute the hashcode of the word
					int machineTarget = word.hashCode() % machineCount;

					//
					FileWriter writer = writers.get(machineTarget);

					if (writer == null) {
						File result = new File(
								directory + "/maps/" + machineTarget + "_" + hashcode);
						writer = new FileWriter(result, true);
						writers.put(machineTarget, writer);
					}

					writer.write(word + " " + 1 + "\n");
				}
			}
			// In order to have a msg in my program, I write the line
			System.out.println(str);
		}

		reader.close();

	}

}
