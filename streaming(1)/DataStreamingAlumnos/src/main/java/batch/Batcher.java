package batch;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Batcher {

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public static void main(String... args) {
		final Runnable beeper = (Runnable) new Batchman();
		final ScheduledFuture<?> beeperHandle = scheduler.scheduleAtFixedRate(beeper, 5, 600, TimeUnit.SECONDS);
		scheduler.schedule(new Runnable() {
			public void run() {
				beeperHandle.cancel(true);
			}
		}, 60 * 60, TimeUnit.SECONDS);
	}
}
