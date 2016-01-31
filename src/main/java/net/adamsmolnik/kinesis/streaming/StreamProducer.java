package net.adamsmolnik.kinesis.streaming;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

/**
 * 
 * @author asmolnik
 *
 */
public final class StreamProducer implements Launchable {

	private final String stream, partitionKey;

	private final AmazonKinesis ak = new AmazonKinesisClient();

	private final Supplier<Optional<byte[]>> supplier;

	private final Thread producer = new Thread() {

		@Override
		public void run() {
			try {
				while (!isInterrupted()) {
					Optional<byte[]> data = supplier.get();
					if (!data.isPresent()) {
						break;
					}
					System.out.println("Before putting record... ");
					ak.putRecord(stream, ByteBuffer.wrap(data.get()), partitionKey);
					TimeUnit.MILLISECONDS.sleep(100);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new StreamingException(e);

			} finally {
				close();
			}

		};

	};

	public StreamProducer(String stream, String partitionKey, Supplier<Optional<byte[]>> supplier) {
		this.stream = stream;
		this.partitionKey = partitionKey;
		this.supplier = supplier;
	}

	@Override
	public void launch() {
		producer.start();
	}

	@Override
	public void close() {
		producer.interrupt();
	}

}
