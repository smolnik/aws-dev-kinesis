package net.adamsmolnik.kinesis.api;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * 
 * @author asmolnik
 *
 */
public class PutGetApiExample {

	public static void main(String[] args) {
		AmazonKinesis ak = new AmazonKinesisClient();
		GetShardIteratorResult si = ak.getShardIterator("kin-test", "000000000001", ShardIteratorType.LATEST.toString());
		GetRecordsRequest getReq = new GetRecordsRequest().withShardIterator(si.getShardIterator()).withLimit(1000);

		PutRecordResult putRes1 = ak.putRecord("kin-test",
				ByteBuffer.wrap(("Message 1. for pk1 at " + LocalDateTime.now()).getBytes(StandardCharsets.UTF_8)), "pk1");
		System.out.println(putRes1);
		PutRecordResult putRes2 = ak.putRecord("kin-test",
				ByteBuffer.wrap(("Message 2. for pk1 at " + LocalDateTime.now()).getBytes(StandardCharsets.UTF_8)), "pk1");
		System.out.println(putRes2);
		while (true) {
			GetRecordsResult getRes = ak.getRecords(getReq);
			List<Record> recs = getRes.getRecords();
			if (!recs.isEmpty()) {
				getRes.getRecords().forEach(r -> {
					ByteBuffer bb = r.getData();
					byte[] arr = new byte[bb.limit()];
					bb.get(arr, 0, arr.length);
					System.out.println(Thread.currentThread() + ": " + new String(arr, StandardCharsets.UTF_8));
				});
				break;
			}
			String next = getRes.getNextShardIterator();
			System.out.println("empty, next: " + next);
			getReq = new GetRecordsRequest().withShardIterator(next).withLimit(10000);
		}
	}

}
