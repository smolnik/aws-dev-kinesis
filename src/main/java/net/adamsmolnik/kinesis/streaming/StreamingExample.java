package net.adamsmolnik.kinesis.streaming;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.amazonaws.http.IdleConnectionReaper;

/**
 * 
 * @author asmolnik
 *
 */
public class StreamingExample {

	public static void main(String[] args) throws Exception {
		AtomicInteger producedCount = new AtomicInteger();
		AtomicInteger consumedCount = new AtomicInteger();

		Consumer<byte[]> c = bb -> {
			consumedCount.incrementAndGet();
			System.out.println("Received: " + new String(bb, StandardCharsets.UTF_8));
		};

		Supplier<Optional<byte[]>> s = () -> {
			producedCount.incrementAndGet();
			return Optional.of(LocalDateTime.now().toString().getBytes(StandardCharsets.UTF_8));
		};

		try (Launchable sc = new StreamConsumer("kinesis-app1", "kin-test", c); Launchable sp = new StreamProducer("kin-test", "pk1", s)) {
			sc.launch();
			TimeUnit.SECONDS.sleep(15);
			sp.launch();
			TimeUnit.SECONDS.sleep(30);
			System.out.println("Before closing... producedCount: " + producedCount + ", consumedCount: " + consumedCount);
			IdleConnectionReaper.shutdown();
		}
	}

}
