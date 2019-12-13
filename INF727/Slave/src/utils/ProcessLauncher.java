package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Lanceur de processus, gestion des erreurs, redirection des threads
 * 
 * @author martinez
 *
 */
public class ProcessLauncher {

	AtomicBoolean status = new AtomicBoolean(true);

	Process process;
	Thread outputStream;
	Thread errorStream;

	List<String> command;

	public ProcessLauncher(String command) {
		this.command = Arrays.asList(command.split(" "));
	}

	private void streamRediction(InputStream input, Consumer<String> output) {
		new BufferedReader(new InputStreamReader(input)).lines().forEach(output);

	}

	protected void handleError(Exception e) {
		if (process.isAlive())
			process.destroy(); // Kill the process
		// As soon as the process is killed, the inputstream while be closed and the
		// thread will end.

		e.printStackTrace(); // Output the error

		status.set(false);
	}

	public boolean runProcess() {
		// Creation du process
		ProcessBuilder pb = new ProcessBuilder(command);

		status.set(true);

		try {
			process = pb.start();

			// Redirection des flux de sorties

			// Avec des linkedProcessQueue
			LinkedBlockingQueue<String> outputQueue = new LinkedBlockingQueue<String>();
			new Thread(() -> streamRediction(process.getInputStream(), t -> {
				try {
					outputQueue.put(t);
				} catch (InterruptedException e1) {
					handleError(e1);
				}
			})).start();

			LinkedBlockingQueue<String> errorQueue = new LinkedBlockingQueue<String>();
			new Thread(() -> streamRediction(process.getErrorStream(), t -> {
				try {
					errorQueue.put(t);
				} catch (InterruptedException e1) {
					handleError(e1);
				}
			})).start();

			int heartBeat = 0;
			// On attendra 5 secondes max.

			while (process.isAlive() && errorQueue.peek() == null) {
				try {
					String output = outputQueue.poll(500, TimeUnit.MILLISECONDS);
					if (output == null) {
						heartBeat++;
						if (heartBeat > 100 && process.isAlive())
							handleError(new TimeoutException("The process wait to long"));

					} else {
						heartBeat = 0;
						outputQueue.clear();
						// System.out.println(output);
					}
				} catch (InterruptedException e) {
					handleError(e);
				}
			}

			String error = errorQueue.peek();
			if (error != null) {
				StringBuilder builder = new StringBuilder();

				while (!errorQueue.isEmpty()) {
					builder.append(errorQueue.poll());
					builder.append("\n");
				}
				error = builder.toString();
				handleError(new Exception(error));

			} 
		} catch (IOException e2) {
			handleError(e2);
		}

		return status.get();
	}

}
