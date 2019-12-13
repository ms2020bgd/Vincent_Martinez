package inf727.clean;

import java.io.IOException;

import inf272.utils.ProcessLauncher;
import inf272.utils.SSHConnexion;

public class Clean {

	public static boolean cleanSlave(String host) {

		// Launch mkdir

		String rmdir = "rm -rf /tmp/vmartinez";

		// Processus sans timeout, le rm peut etre long
		ProcessLauncher launcher = new ProcessLauncher(SSHConnexion.SSH + host + " " + rmdir, false);
		boolean status = launcher.runProcess();

		if (!status) {
			System.err.println(host + " is not alive");
			return status;
		}
		return status;
	}

	/*
	 * 
	 */
	public static void main(String[] args) throws IOException {

		SSHConnexion connexion = new SSHConnexion();
		connexion.getHostList().parallelStream().forEach(Clean::cleanSlave);
	}

}
