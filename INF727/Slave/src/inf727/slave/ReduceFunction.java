package inf727.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

public class ReduceFunction {

	String directory = "/tmp/vmartinez/shuffle";
	HashMap<String, Integer> maps = new HashMap<String, Integer>();

	public ReduceFunction() {

	}

	private void wordCount(File f) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(f));
		String str = null;
		while ((str = reader.readLine()) != null) {
			String tokens[] = str.split(" ");
			String word = tokens[0];
			if (!word.isEmpty()) {
				Integer count = maps.get(word);
				if (count == null) {
					count = 1;

				} else {
					count++;
				}
				maps.put(word, count);

			}
			// In order to have a msg in my program, I write the line
			//System.out.println(str);
		}
		
		
		reader.close();

	}

	void processedReduce() throws IOException {
		File f = new File(directory);

		// On procède a tous les fichiers de shuffle
		Arrays.asList(f.listFiles()).stream().forEach(l -> {
			try {
				System.out.println("Reducing "+l);
				wordCount(l);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		f = new File("/tmp/vmartinez/result");
		FileWriter writer = new FileWriter(f);
		for (Entry<String, Integer> entry : maps.entrySet()) {
			writer.write(entry.getKey() + " " + entry.getValue() + "\n");
		}

		writer.close();

	}
}
