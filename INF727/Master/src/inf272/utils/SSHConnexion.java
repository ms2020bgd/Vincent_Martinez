package inf272.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SSHConnexion {

	final public static String SSH = "ssh vmartinez@";
	protected List<String> host = new ArrayList();

	public SSHConnexion() {

	}

	public List<String> getHostList() {
		if (host.isEmpty()) {
			File f = new File("data/machines.txt");
			if (f.exists())
				try {
					host = readFile(f);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return host;
	}

	protected List<String> readFile(File f) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(f));
		List<String> list = new ArrayList<>();
		String str = null;
		while ((str = reader.readLine()) != null) {
			if (!str.trim().isEmpty())
				list.add(str.trim());
		}
		reader.close();
		return list;
	}
}
