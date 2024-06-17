package rs.raf.pds.faulttolerance;

import rs.raf.pds.faulttolerance.gRPC.AccountServiceGrpc;

/**
 * Klasa koja predstavlja gRPC kanal za sledbenika u sistemu.
 */
public class FollowerGRPCChannel {
	 final String zkNode;
	 final String connectionString;
	 final AccountServiceGrpc.AccountServiceBlockingStub blockingStub;

	/**
	 * Konstruktor za FollowerGRPCChannel.
	 *
	 * @param zkNode          Zookeeper čvor koji predstavlja sledbenika.
	 * @param connectionString gRPC konekcioni string.
	 * @param blockingStub     gRPC blokirajući stub za komunikaciju sa sledbenikom.
	 */
	public FollowerGRPCChannel(String zkNode, String connectionString, AccountServiceGrpc.AccountServiceBlockingStub blockingStub) {
		this.zkNode = zkNode;
		this.connectionString = connectionString;
		this.blockingStub = blockingStub;
	}

	/**
	 * Vraća Zookeeper čvor.
	 *
	 * @return Zookeeper čvor.
	 */
	public String getZkNode() {
		return zkNode;
	}

	/**
	 * Vraća gRPC konekcioni string.
	 *
	 * @return gRPC konekcioni string.
	 */
	public String getConnectionString() {
		return connectionString;
	}

	/**
	 * Vraća gRPC blokirajući stub.
	 *
	 * @return gRPC blokirajući stub.
	 */
	public AccountServiceGrpc.AccountServiceBlockingStub getBlockingStub() {
		return blockingStub;
	}
}
