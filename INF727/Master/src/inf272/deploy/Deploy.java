package inf272.deploy;

import java.io.File;
import java.io.IOException;
import java.util.List;

import inf272.utils.ProcessLauncher;
import inf272.utils.SSHConnexion;

public class Deploy {

	public Deploy() {

	}

	public boolean deployFile(String host, File f) {
		// Launch mkdir

		String mkdir = "mkdir -p /tmp/vmartinez/shuffle";

		ProcessLauncher launcher = new ProcessLauncher(SSHConnexion.SSH + host + " " + mkdir);
		boolean status = launcher.runProcess();

		
		
		if (!status) {
			System.err.println(host + " is not alive");
			return status;
		}

		// Copy the slave
		if (f.exists()) {
			String scp = "scp " + f.getAbsolutePath() + " vmartinez@" + host + ":/tmp/vmartinez/";
			// Let's copy!
			launcher = new ProcessLauncher(scp);
			status = launcher.runProcess();

			if (!status) {
				System.err.println(host + " is not alive");
				return status;
			}
		}
		return status;

	}

	final static String SLAVE = "data/slave.jar";

	public boolean deploySlave(String host) {

		return deployFile(host, new File(SLAVE));
	}

	/*
	 * 
	 */
	public static void main(String[] args) throws IOException {

		SSHConnexion connexion = new SSHConnexion();
		List<String> hostList = connexion.getHostList();

//		hostList.parallelStream().forEach(Deploy::deploySlave);
	}
}
