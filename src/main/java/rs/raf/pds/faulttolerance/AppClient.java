package rs.raf.pds.faulttolerance;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import rs.raf.pds.faulttolerance.gRPC.AccountRequest;
import rs.raf.pds.faulttolerance.gRPC.AccountRequestType;
import rs.raf.pds.faulttolerance.gRPC.AccountResponse;
import rs.raf.pds.faulttolerance.gRPC.AccountServiceGrpc;
import rs.raf.pds.faulttolerance.gRPC.RequestStatus;
import rs.raf.pds.faulttolerance.core.SyncPrimitive;

public class AppClient extends SyncPrimitive {

	final String appRoot;
	String leaderNodeName = null;
	String leaderHostNamePort;

	ManagedChannel channel = null;
	AccountServiceGrpc.AccountServiceBlockingStub blockingStub = null;

	AccountServiceGrpc.AccountServiceBlockingStub followerBlockingStub = null;
	List<String> list = new ArrayList<>();

	protected AppClient(String zkAddress, String appRoot) throws KeeperException, InterruptedException {
		super(zkAddress);
		this.appRoot = appRoot;
	}
	@Override
	public void process(WatchedEvent event) {
		try {
			checkLeader();
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Check is leader is active.
	 * Checks the client no recognizes the leader.
	 * If it doesn't recognize the leader, it tries to find a new leader.
	 * **/
	public synchronized void checkLeader() throws KeeperException, InterruptedException {
		//Thread.sleep(100);
		list = zk.getChildren(appRoot, false);
		System.out.println("There are total:"+list.size()+ " replicas for elections!");
		for (int i=0; i<list.size(); i++)
			System.out.print("NODE:"+list.get(i)+", ");
		System.out.println();

		if (list.size() == 0) {
			System.out.println("0 Elemenata ? ");
		} else {
			Integer minValue = Integer.parseInt(list.get(0).substring(AppServer.REPLICA_NODE_SEQUENCE_INDEX));
			String minNodeName = list.get(0);

			for(int i=1; i<list.size(); i++) {
				Integer tempValue = Integer.parseInt(list.get(i).substring(AppServer.REPLICA_NODE_SEQUENCE_INDEX));
				if(minValue > tempValue) {
					minValue = tempValue;
					minNodeName = list.get(i);
				}
			}
			if (leaderNodeName == null || !minNodeName.equals(leaderNodeName)) {
				leaderNodeName = minNodeName;
				byte[] b = zk.getData(appRoot + "/" + leaderNodeName, true, null);
				leaderHostNamePort = new String(b);

				System.out.println("Leader je " + leaderNodeName);

				// Check if the leader is active
				boolean isLeaderActive = isLeaderActive(leaderHostNamePort);
				if (!isLeaderActive) {
					System.out.println("Leader is not active yet. Waiting...");

					Thread.sleep(3000);
					checkLeader();
				}
				else {
					// leader is active. Create a blocking stub
					blockingStub = getBlockingStub(leaderHostNamePort);
				}

			}
			for (String nodeName : list) {
				if (!nodeName.equals(leaderNodeName)) {
					byte[] followerData = zk.getData(appRoot + "/" + nodeName, true, null);
					String followerHostNamePort = new String(followerData);
					// Create a blocking stub for each follower
					followerBlockingStub = getBlockingStub(followerHostNamePort);
					// Send a request to each follower

				}
			}

		}
	}


	/**
	 * Check if leader is active.
	 * **/
	private boolean isLeaderActive(String leaderHostNamePort) {
		ManagedChannel channel = null;
		try {
			String[] splits = leaderHostNamePort.split(":");
			channel = ManagedChannelBuilder.forAddress(splits[0], Integer.parseInt(splits[1]))
					.usePlaintext()
					.build();

			AccountServiceGrpc.AccountServiceBlockingStub blockingStub = AccountServiceGrpc.newBlockingStub(channel);

			// Attempt to send some request to the leader, e.g., getAmount
			AccountRequest request = AccountRequest.newBuilder()
					.setRequestId(1)
					.setOpType(AccountRequestType.GET)
					.build();

			AccountResponse response = blockingStub.getAmount(request);

			// Check if the response came from the leader and if the response is correct
			if (response != null && response.getStatus() == RequestStatus.STATUS_OK) {
				System.out.println("Leader is active.");
				return true;
			} else {
				System.out.println("Leader is not active.");
				return false;
			}
		} catch (Exception e) {
			System.out.println("Error while checking leader activity: " + e.getMessage());
			return false;
		} finally {
			// Ensure proper shutdown of the channel
			if (channel != null) {
				channel.shutdown();
			}
		}
	}


	public  AccountServiceGrpc.AccountServiceBlockingStub getBlockingStub(String hostNamePort){
		String[] splits = hostNamePort.split(":");
		channel = ManagedChannelBuilder.forAddress(splits[0], Integer.parseInt(splits[1]))
				.usePlaintext()
				.build();

		return AccountServiceGrpc.newBlockingStub(channel);
	}
	private void inviteServer() throws KeeperException, InterruptedException {
		for (int i=0; i<10; i++) {
			try {

				inviteServerFunctions(blockingStub);

				Thread.sleep(2000);

				System.out.println("Leader je "+leaderNodeName);

			}catch(RuntimeException e) {
				System.out.println("Server has crashed!");
				e.printStackTrace();
				//channel.shutdown();

				checkLeader();

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		channel.shutdown();
	}
	public static void main(String[] args) {

		if (args.length != 1) {
			System.out.println("Usage java -cp 'target/PDS-FT1-1.0.jar:target/classes:target/lib/*' rs.raf.pds.faulttolerance.AppClient localhost:2181");
			System.exit(1);
		}

		AppClient client;
		try {
			client = new AppClient(args[0], AppServer.APP_ROOT_NODE);
			client.checkLeader();
			client.inviteServer();


		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}

	private void inviteServerFunctions(AccountServiceGrpc.AccountServiceBlockingStub blockingStub) {
		// Calling AppServer's over GRPC
		System.out.println("Prvi poziv je getAmount()");
		AccountRequest request = AccountRequest.newBuilder()
				.setRequestId(1)
				.setOpType(AccountRequestType.GET)
				.build();

		AccountResponse response;
		AccountResponse[] followerResponses;

		synchronized(this) {
			response = blockingStub.getAmount(request);

			// Initialize followerResponses array
			// 1 less becase leader is not a follower
			followerResponses = new AccountResponse[list.size() - 1];

			int followerIndex = 0;


			for (String followerNodeName : list) {
				if (!followerNodeName.equals(leaderNodeName)) {

					AccountRequest followerRequest = AccountRequest.newBuilder()
							.setRequestId(1)
							.setOpType(AccountRequestType.GET)
							.build();


					AccountResponse followerResponse = followerBlockingStub.getAmount(followerRequest);


					followerResponses[followerIndex++] = followerResponse;
				}
			}
		}


		ispisResponse(response, request);


		for (AccountResponse followerResponse : followerResponses) {
			ispisResponse(followerResponse, request);
		}

		System.out.println("Drugi poziv je addAmount()");
		request = AccountRequest.newBuilder()
				.setRequestId(2)
				.setOpType(AccountRequestType.ADD)
				.setAmount(150.0f)
				.build();
		synchronized(this) {
			response = blockingStub.addAmount(request);
		}
		ispisResponse(response, request);

		System.out.println("Treci poziv je withdrawAmount()");
		request = AccountRequest.newBuilder()
				.setRequestId(3)
				.setOpType(AccountRequestType.WITDRAWAL)
				.setAmount(70.0f)
				.build();

		synchronized(this) {
			response = blockingStub.witdrawAmount(request);
		}
		ispisResponse(response, request);

		System.out.println("Cetvrti poziv je withdrawAmount()");
		request = AccountRequest.newBuilder()
				.setRequestId(4)
				.setOpType(AccountRequestType.WITDRAWAL)
				.setAmount(100.0f)
				.build();
		synchronized(this) {
			response = blockingStub.witdrawAmount(request);
		}
		ispisResponse(response, request);
	}


	public static void ispisResponse(AccountResponse response, AccountRequest request) {
		if (response.getStatus() == RequestStatus.STATUS_OK) {
			System.out.println("STATUS OK! REQUEST = "+request.getOpType()+ ", AMOUNT = "+response.getBalance());
		}else if (response.getStatus() == RequestStatus.UPDATE_REJECTED_NOT_LEADER) {
			System.out.println("UPDATE_REJECTED_NOT_LEADER! "+request.getOpType().toString());
		}
		else if (response.getStatus() == RequestStatus.WITDRAWAL_REJECT_NOT_SUFFICIENT_AMOUNT) {
			System.out.println(".WITDRAWAL_REJECT_NOT_SUFFICIENT_AMOUNT! Amount"+request.getAmount());
		}

		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
