package inf727.slave;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import utils.ProcessLauncher;

public class ShuffleFunction {

	String directory;
	List<String> reducer;

	public ShuffleFunction(String directory, List<String> reducer) {

		this.directory = directory;
		this.reducer = reducer;

	}


	public void processedShuffle() throws IOException {
		// On va lire le fichier des PC sur dans le repertoire
		File f = new File(directory + "/maps");


		Arrays.asList(f.listFiles()).parallelStream().forEach(l -> {
			String splits = l.getName().split("_")[0];
			// Convert to int
			int value = Integer.valueOf(splits);
			// Modulus from the pcs count
			int index = (int) (value % reducer.size());
			// Choose the corresponding pc

			String host = reducer.get(index);
			// Send the file to the corresponding directory

			try {
				if (!InetAddress.getLocalHost().equals(InetAddress.getByName(host))) {
					String scp = "scp " + l.getAbsolutePath() + " vmartinez@" + host + ":/tmp/vmartinez/shuffle/";

					// Let's copy!
					ProcessLauncher launcher = new ProcessLauncher(scp);
					boolean status = launcher.runProcess();

					if (!status) {
						System.err.println(host + " is not alive");
					}
				}
				else
				{
					String scp = "cp " + l.getAbsolutePath()  + " /tmp/vmartinez/shuffle/";

					// Let's copy!
					ProcessLauncher launcher = new ProcessLauncher(scp);
					boolean status = launcher.runProcess();

					if (!status) {
						System.err.println(host + " is not alive");
					}
					
				}
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		});

	}

}
