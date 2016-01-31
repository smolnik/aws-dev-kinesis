package net.adamsmolnik.kinesis.streaming;

import java.util.function.Consumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * 
 * @author asmolnik
 *
 */
public class StreamConsumer implements Launchable {

	private final Worker worker;

	private final Thread launcher = new Thread() {

		@Override
		public void run() {
			try {
				worker.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

	};

	public StreamConsumer(String appName, String stream, Consumer<byte[]> consumer) {
		String workerId = "worker:" + getClass().getSimpleName() + "@" + System.identityHashCode(this);
		this.worker = new Worker(() -> new MessageProcessor(consumer),
				new KinesisClientLibConfiguration(appName, stream, new DefaultAWSCredentialsProviderChain(), workerId));
	}

	@Override
	public void launch() {
		launcher.start();
	}

	@Override
	public void close() {
		worker.shutdown();
	}

}
