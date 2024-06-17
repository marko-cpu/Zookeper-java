package rs.raf.pds.faulttolerance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.util.Scanner;

import rs.raf.pds.faulttolerance.commands.AddValueCommand;
import rs.raf.pds.faulttolerance.commands.Command;
import rs.raf.pds.faulttolerance.commands.SubValueCommand;
import rs.raf.pds.faulttolerance.gRPC.AccountRequest;
import rs.raf.pds.faulttolerance.gRPC.AccountResponse;
import rs.raf.pds.faulttolerance.gRPC.LogResponse;
import rs.raf.pds.faulttolerance.gRPC.LogStatus;
import rs.raf.pds.faulttolerance.gRPC.RequestStatus;

public class AccountService {

	ByteArrayInputStream bais;
	ByteArrayOutputStream baos = new ByteArrayOutputStream();

	DataInputStream dis = new DataInputStream(bais);
	DataOutputStream dos = new DataOutputStream(baos);


	boolean isLeader = false;
	float amount = 0.0f;
	ReplicatedLog log;

	public AccountService(ReplicatedLog log) {
		this.log = log;
	}
	public float addAmount(float value, boolean applyLog) {

		if (applyLog) {
			AddValueCommand command = new AddValueCommand(value);
			appendCommandToLogAndReplicate(command);
		}
		amount += value;

		return amount;
	}
	public float witdrawAmount(float value, boolean applyLog) {

		if (amount>=value) {

			if (applyLog) {
				SubValueCommand command = new SubValueCommand(value);
				appendCommandToLogAndReplicate(command);
			}

			amount -= value;

			return amount;

		}else
		{
			return -1;

		}

	}


	public AccountResponse getAmount(AccountRequest req) {
		AccountResponse resp = AccountResponse.newBuilder()
				.setRequestId(req.getRequestId())
				.setBalance(amount)
				.setStatus(RequestStatus.STATUS_OK)
				.build();

		return resp;
	}
	public void appendCommandToLogAndReplicate(Command command) {
		try {
			//command.serialize(dos);
			//log.appendAndReplicate(baos.toByteArray());
			//baos.reset();

			log.appendAndReplicate(command.writeToString().getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public LogResponse appendLog(Long entryIndex, byte[] data) throws IOException {
		if (log.getLastLogEntryIndex()<(entryIndex-1)) {
			LogResponse response = LogResponse.newBuilder().
					setStatus(LogStatus.LOG_HASNT_LAST_ENTRY).
					setLastEntryIndex(log.lastLogEntryIndex).
					setEntryAtIndex(entryIndex).
					build();
			return response;
		}

		log.appendToLocalLog(data);

		//DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));
		Scanner sc = new Scanner(new String(data));

		//int commandType = ds.readInt();
		int commandType = sc.nextInt();

		if (commandType == Command.AddValueType) {
			//AddValueCommand command = AddValueCommand.deserialize(ds);
			AddValueCommand command = new AddValueCommand(sc.nextFloat());
			addAmount(command.getValue(), false);

		}else if (commandType == Command.SubValueType) {
			//SubValueCommand command = SubValueCommand.deserialize(ds);
			SubValueCommand command = new SubValueCommand(sc.nextFloat());
			witdrawAmount(command.getValue(), false);
		}

		LogResponse response = LogResponse.newBuilder().
				setStatus(LogStatus.LOG_OK).
				setEntryAtIndex(entryIndex).
				build();

		return response;
	}

	public void setServerState(boolean isLeader) {
		this.isLeader = isLeader;
	}


}
