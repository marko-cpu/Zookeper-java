package rs.raf.pds.faulttolerance;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ReplicatedLog {

	public static interface LogReplicator {
		public void replicateOnFollowers(Long entryAtIndex, byte[] data);
	}

	Long lastLogEntryIndex = 0L;
	final LogReplicator node;
	FileOutputStream fs;
	OutputStreamWriter writer;

	public ReplicatedLog(String fileName, LogReplicator node) throws FileNotFoundException {
		this.node = node;
		fs = new FileOutputStream(fileName);
		writer = new OutputStreamWriter(fs);
	}

	public void appendAndReplicate(byte[] data) throws IOException {
		Long lastLogEntryIndex = appendToLocalLog(data);
		node.replicateOnFollowers(lastLogEntryIndex, data);
	}

	protected Long appendToLocalLog(byte[] data) throws IOException {
		String s = new String(data);
		System.out.println("Log #"+lastLogEntryIndex+":"+s);

		//fs.write(data);
		//fs.flush();
		writer.write(s);writer.write("\r\n");
		writer.flush();
		fs.flush();

		return ++lastLogEntryIndex;
	}

	protected Long getLastLogEntryIndex() {
		return lastLogEntryIndex;
	}

}
