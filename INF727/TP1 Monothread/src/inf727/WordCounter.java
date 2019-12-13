package inf727;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class WordCounter {

	/**
	 * Lecture du fichier et compatge des mots
	 * 
	 * @param f a file
	 * @return an HashMap: Key= Words, Value = WordCount
	 * @throws IOException
	 */
	public HashMap<String, Integer> readFile(File f) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(f));
		HashMap<String, Integer> maps = new HashMap<>();
		String str = null;
		while ((str = reader.readLine()) != null) {
			StringTokenizer token = new StringTokenizer(str, " \t\n\r\f,;.:!?\"'");
			while (token.hasMoreTokens()) {
				String word = token.nextToken();
				Integer count = maps.get(word);
				if (count == null) {
					maps.put(word, 1);
				} else {
					maps.put(word, count + 1);
				}

			}
		}
		reader.close();

		return maps;

	}

	/**
	 * Lecture en utilisant les streams
	 * 
	 * @param f
	 * @return
	 * @throws IOException
	 */
	public Map<String, Integer> readFileNio(File f) throws IOException {

		long time1 = System.currentTimeMillis();
		Map<String, Integer> maps = Files.lines(f.toPath())
				.flatMap(l -> Arrays.stream(l.split("\\s|\\t|\\n|\\r|\\f|\\,|\\;|\\.|\\:|\\!|\\?|\\\"|\\'")))
				.collect(Collectors.toMap(w -> w, w -> 1, Integer::sum));

		long time2 = System.currentTimeMillis();
		System.err.println("Temps de lecture/ comptage= " + (time2 - time1));

		return maps;

	}

	/**
	 * Tri des mots par comparateur
	 * 
	 * @param words
	 * @return
	 */
	List<Entry<String, Integer>> getSortedValues(Map<String, Integer> words) {
		List<Entry<String, Integer>> sortedList = new ArrayList<>(words.entrySet());
		Collections.sort(sortedList, new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {

				int val = -(e1.getValue().compareTo(e2.getValue()));

				if (val == 0)
					val = e1.getKey().compareTo(e2.getKey());

				return val;
			}
		});

		return sortedList;
	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		assert (args.length == 1);

		File f = new File(args[0]);
		if (!f.exists()) {
			System.err.println("The specified file doesn't exist");
			System.exit(1);
		} else {
			
			
			WordCounter counter = new WordCounter();
			long time1 = System.currentTimeMillis();
			Map<String, Integer> words = counter.readFile(f);

		//	Map<String, Integer> words = counter.readFileNio(f);
			long time2 = System.currentTimeMillis();
			System.out.println("Temps de lecture/ comptage= " + (time2 - time1));
			// Tri par values

			long t1 = System.currentTimeMillis();
			List<Entry<String, Integer>> sortedList = counter.getSortedValues(words);

			System.out.println("Temps de tri par clé/valeur via liste = " + (System.currentTimeMillis() - t1));

			t1 = System.currentTimeMillis();

			List<Entry<String, Integer>> list = words.entrySet().stream()
					.sorted((Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) -> {
						int val = e1.getValue().compareTo(e2.getValue());
						return val == 0 ? e1.getKey().compareTo(e2.getKey()) : -val;
					}).collect(Collectors.toList());

			System.out.println("Temps de tri par clé/valeur via stream = " + (System.currentTimeMillis() - t1));

			System.out.println("Word    Count");
			for (int i = 0; i < 5; i++) {
				Entry<String, Integer> entry = sortedList.get(i);
				System.out.println(entry.getKey() + " " + entry.getValue());
			}
			System.out.println();
			
			System.out.println("Word    Count");
			for (int i = 0; i < 50; i++) {
				Entry<String, Integer> entry = list.get(i);
				System.out.println(entry.getKey() + " " + entry.getValue());
			}
			
			

		}
	}

}
