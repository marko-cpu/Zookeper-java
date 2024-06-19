	package rs.raf.pds.faulttolerance;

	import java.io.*;

	public class ReplicatedLog {

		public static interface LogReplicator {
			public void replicateOnFollowers(Long entryAtIndex, byte[] data);
		}

		Long lastLogEntryIndex = 	0L;
		final LogReplicator node;
		FileOutputStream fs;
		OutputStreamWriter writer;

		public ReplicatedLog(String fileName, LogReplicator node) throws FileNotFoundException {
			this.node = node;
			fs = new FileOutputStream(fileName, true);
			writer = new OutputStreamWriter(fs);
		}

		public void appendAndReplicate(byte[] data) throws IOException {
			Long lastLogEntryIndex = appendToLocalLog(data);
			node.replicateOnFollowers(lastLogEntryIndex, data);
		}


		protected Long appendToLocalLog(byte[] data) throws IOException {
			String logEntry = new String(data);
			System.out.println("Log #" + lastLogEntryIndex + ": " + logEntry);

//			// Check if Follower can append the log
//			if (lastLogEntryIndex == 0) {
//				System.out.println("Follower cannot append to log without previous entry.");
//				return null;
//			}

			if (lastLogEntryIndex == 0) {
				System.out.println("Appending first log entry.");
				writer.write(logEntry);
				writer.write("\r\n");
				writer.flush();
				fs.flush();
				return ++lastLogEntryIndex;
			}

			writer.write(logEntry);
			writer.write("\r\n");
			writer.flush();
			fs.flush();

			return ++lastLogEntryIndex;
		}

		protected Long getLastLogEntryIndex() {
			return lastLogEntryIndex;
		}
	/**
	 * Saving snapshot to file
	 * **/
		public synchronized void takeSnapshot() {
			String snapshotFilePath = "/home/marko/Documents/GitHub/Zookeper-java/Snapshot/replicatedSnapshot.ser";
			try {
				FileOutputStream snapshotOutputStream = new FileOutputStream(snapshotFilePath);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(snapshotOutputStream);
				objectOutputStream.writeObject(getLastLogEntryIndex());
				objectOutputStream.close();
				snapshotOutputStream.close();
				System.out.println("Snapshot taken at log entry index: " + lastLogEntryIndex);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		  * Loading snapshot from file
		 * **/
		public void loadSnapshot() {
			String snapshotFilePath = "/home/marko/Documents/GitHub/Zookeper-java/Snapshot/replicatedSnapshot.ser";
			try {
				FileInputStream fileIn = new FileInputStream(snapshotFilePath);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				this.lastLogEntryIndex = (Long) in.readObject();
				System.out.println("Loaded snapshot at log entry index: " + lastLogEntryIndex);
				in.close();
				fileIn.close();
			} catch (IOException | ClassNotFoundException i) {
				i.printStackTrace();

			}


		}


	}
