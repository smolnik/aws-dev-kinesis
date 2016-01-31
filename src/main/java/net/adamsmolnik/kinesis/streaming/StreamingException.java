package net.adamsmolnik.kinesis.streaming;

/**
 * 
 * @author asmolnik
 *
 */
public class StreamingException extends RuntimeException {

	private static final long serialVersionUID = 3950352828196721407L;

	public StreamingException(Exception e) {
		super(e);
	}

}
