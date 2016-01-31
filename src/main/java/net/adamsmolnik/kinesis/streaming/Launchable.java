package net.adamsmolnik.kinesis.streaming;

/**
 * 
 * @author asmolnik
 *
 */
public interface Launchable extends AutoCloseable {

	void launch();

	void close();

}
