package net.adamsmolnik.kinesis.streaming;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

/**
 * 
 * @author asmolnik
 *
 */
public class MessageProcessor implements IRecordProcessor {

	private final Consumer<byte[]> consumer;

	public MessageProcessor(Consumer<byte[]> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void initialize(String s) {
	}

	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		records.forEach(r -> {
			ByteBuffer bb = r.getData();
			byte[] data = new byte[bb.limit()];
			bb.get(data, 0, data.length);
			consumer.accept(data);
		});
	}

	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason sr) {
	}

}
