package net.adamsmolnik.kinesis.streaming;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * 
 * @author asmolnik
 *
 */
public class GetApiExample {

	public static void main(String[] args) {
		AmazonKinesis ak = new AmazonKinesisClient();
		GetShardIteratorResult si = ak.getShardIterator("kin-test", "000000000001", ShardIteratorType.TRIM_HORIZON.toString());
		GetRecordsRequest getReq = new GetRecordsRequest().withShardIterator(si.getShardIterator()).withLimit(1000);
		GetRecordsResult getRes = ak.getRecords(getReq);
		getRes.getRecords().forEach(r -> {
			ByteBuffer bb = r.getData();
			byte[] arr = new byte[bb.limit()];
			bb.get(arr, 0, arr.length);
			System.out.println(Thread.currentThread() + ": " + new String(arr, StandardCharsets.UTF_8));
		});
	}

}
