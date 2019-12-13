package inf727.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Slave {

	private static List<String> readMachineFile() throws IOException {
		File f = new File("/tmp/vmartinez/machines.txt");
		BufferedReader reader = new BufferedReader(new FileReader(f));
		List<String> list = new ArrayList<>();
		String str = null;
		while ((str = reader.readLine()) != null) {
			list.add(str.trim());
		}
		reader.close();
		return list;
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("You must specified Map, Shuffle or Reduce");
		}

		switch (args[0]) {
		case "Map":
			MapFunction map = new MapFunction("/tmp/vmartinez",readMachineFile().size());
			map.processedMap();

			break;
		case "Shuffle":
			ShuffleFunction shuffle = new ShuffleFunction("/tmp/vmartinez", readMachineFile());
			shuffle.processedShuffle();
			break;

		case "Reduce":
			ReduceFunction reduce = new ReduceFunction();
			reduce.processedReduce();
			break;
		default:
			System.err.println("You must specified Map, Shuffle or Reduce");
		}
	}
}
