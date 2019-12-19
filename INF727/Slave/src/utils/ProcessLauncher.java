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

	/**
	 * Etat de sortie du process
	 */
	AtomicBoolean status = new AtomicBoolean(true);

	Process process;
	Thread outputStream;
	Thread errorStream;

	List<String> command;

	public ProcessLauncher(String command) {
		this.command = Arrays.asList(command.split(" "));
	}

	/**
	 * Redirection d'un flux sur un consumer
	 * 
	 * @param input
	 * @param output
	 */
	private void streamRediction(InputStream input, Consumer<String> output) {
		new BufferedReader(new InputStreamReader(input)).lines().forEach(output);

	}

	/**
	 * Gestion des erreurs - On tue le process si il est toujoursen vie - On affiche
	 * la stack
	 * 
	 * @param e
	 */
	protected void handleError(Exception e) {

		e.printStackTrace(); // Output the error

		if (process.isAlive())
			process.destroy(); // Kill the process
		// As soon as the process is killed, the inputstream while be closed and the
		// thread will end.

		status.set(false);
	}

	/**
	 * Lancement du process, boucle acive et code de retour
	 * 
	 * @return
	 */
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

			// Battement de coeur. On attendra pendant maxHearth fois 500 ms
			int hearthBeat = 0;
			int maxHearthBeat = 100; // On attendra 5 secondes max.

			while (process.isAlive() && errorQueue.peek() == null) {
				try {
					String output = outputQueue.poll(500, TimeUnit.MILLISECONDS);
					if (output == null) {
						hearthBeat++;
						if (hearthBeat > maxHearthBeat && process.isAlive())
							handleError(new TimeoutException("The process wait to long"));

					} else {
						// Si on a du monde sur la sortie standard, on clear tout cela et on repasse
						// dans la boucle
						hearthBeat = 0;
						outputQueue.clear();

					}
				} catch (InterruptedException e) {
					handleError(e);
				}
			}

			// On verifie que l'on n'a rien sur la sortie erreur
			String error = errorQueue.peek();
			if (error != null) {
				StringBuilder builder = new StringBuilder();

				while (!errorQueue.isEmpty()) {
					builder.append(errorQueue.poll());
					builder.append("\n");
				}
				error = builder.toString();
				// On lance une erreur
				handleError(new Exception(error));

			}
		} catch (IOException e2) {
			// En cas d'exception, lancement d'une erreur et sortie
			handleError(e2);
		}

		return status.get();
	}

}
