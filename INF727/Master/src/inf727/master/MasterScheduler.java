package inf727.master;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import inf272.deploy.Deploy;
import inf272.utils.ProcessLauncher;
import inf272.utils.SSHConnexion;
import inf272.utils.SplitFile;
import inf727.clean.Clean;

/**
 * The main purpose of this class is to schedule and fix problems
 * 
 * @author martinez
 *
 */
public class MasterScheduler {

	String fileName;

	/**
	 * 
	 * @param fileName
	 */
	public MasterScheduler(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * Return the available machine from the list
	 * 
	 * @return
	 */
	protected List<String> getAvailables() {
		SSHConnexion connexion = new SSHConnexion();

		List<String> host = connexion.getHostList();

		List<String> availablePCs = host.parallelStream()
				.filter(l -> new ProcessLauncher(SSHConnexion.SSH + l + " hostname").runProcess())
				.collect(Collectors.toCollection(ArrayList::new));

		return availablePCs;
	}

	/**
	 * Clean and deploy slave.jar on each slave. Return true if ok, false either
	 * 
	 * @param availablePCs
	 * @return
	 */
	protected List<Boolean> cleanAndDeploy(List<String> availablePCs) {
		return availablePCs.parallelStream().map(l -> {
			boolean ok = Clean.cleanSlave(l);
			if (ok) {
				Deploy deploy = new Deploy();
				ok = deploy.deploySlave(l);
			}
			return ok;
		}).collect(Collectors.toCollection(ArrayList::new));
	}

	/**
	 * Deploy file on each pc Each file contains a "split"
	 * 
	 * @param list
	 * @return
	 */
	protected List<Boolean> deployFileOnMachine(List<String> availablePCs, List<File> files) {
		Thread threads[] = new Thread[availablePCs.size()];
		List<Boolean> returnVal = new ArrayList<Boolean>();

		for (int i = 0; i < availablePCs.size(); i++) {
			final int idx = i;
			Thread tread = new Thread(() -> {
				
				Deploy deploy = new Deploy();
				returnVal.add(deploy.deployFile(availablePCs.get(idx), files.get(idx)));
			});

			tread.start();
			threads[i] = tread;
		}

		for (Thread th : threads)
			try {
				th.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return returnVal;
	}

	/**
	 * @throws IOException
	 * 
	 */
	public void processMapReduce() throws IOException {
		// First step, checking the slave's PC
		List<String> availablePCs = getAvailables();

		// Second, split and copy
		// The file is given in the main
		
		long t1 = System.currentTimeMillis();		
		SplitFile splitFile = new SplitFile(availablePCs.size());
		List<File> files = splitFile.splitFile(new File(fileName));				
		long t2 = System.currentTimeMillis();		
		System.out.println("Time for file splitting="+ (t2-t1)/1000.0);
		
		
		// Let's clean and deploy slave
		cleanAndDeploy(availablePCs);
		
		t1 = System.currentTimeMillis();
		long timeGlobal = t1;
		// Deploy the splits
		deployFileOnMachine(availablePCs, files);
		
		t2 = System.currentTimeMillis();		
		System.out.println("Time for file deploying="+ (t2-t1)/1000.0);
		
		
		// Run all the slave; giving them the all PC
		// Write the pcs in a file
		FileWriter f = new FileWriter(new File("machines.txt"));

		for (String pc : availablePCs)
			f.write(pc + "\n");
		f.close();

		availablePCs.parallelStream().forEach(l -> {
			Deploy deploy = new Deploy();
			deploy.deployFile(l, new File("machines.txt"));
		});

		t1 = System.currentTimeMillis();
		
		availablePCs.parallelStream().map(l -> {
			ProcessLauncher launcher = new ProcessLauncher(
					SSHConnexion.SSH + l + " java -cp /tmp/vmartinez/slave.jar inf727.slave.Slave Map");
			return launcher.runProcess();
		}).collect(Collectors.toCollection(ArrayList::new));
		t2 = System.currentTimeMillis();		
		System.out.println("Time for Map function="+ (t2-t1)/1000.0);

		t1 = System.currentTimeMillis();
		availablePCs.parallelStream().map(l -> {
			ProcessLauncher launcher = new ProcessLauncher(
					SSHConnexion.SSH + l + " java -cp /tmp/vmartinez/slave.jar inf727.slave.Slave Shuffle");
			return launcher.runProcess();
		}).collect(Collectors.toCollection(ArrayList::new));		
		t2 = System.currentTimeMillis();		
		System.out.println("Time for Shuffle function="+ (t2-t1)/1000.0);

		t1 = System.currentTimeMillis();
		availablePCs.parallelStream().map(l -> {
			ProcessLauncher launcher = new ProcessLauncher(
					SSHConnexion.SSH + l + " java -cp /tmp/vmartinez/slave.jar inf727.slave.Slave Reduce");
			return launcher.runProcess();
		}).collect(Collectors.toCollection(ArrayList::new));
		t2 = System.currentTimeMillis();		
		System.out.println("Time for Reduce function="+ (t2-t1)/1000.0);
		
		System.out.println("The global time for copy, map and reduce="+(t2-timeGlobal)/1000.0);
	}

}
