package inf727.master;

import java.io.IOException;

public class Master {

	public static void main(String[] args) throws IOException, InterruptedException {
		MasterScheduler scheduler = new MasterScheduler(args[0]);
		scheduler.processMapReduce();
	}

}
