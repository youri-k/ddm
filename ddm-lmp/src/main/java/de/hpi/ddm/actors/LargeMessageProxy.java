package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.io.IOException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import com.twitter.chill.KryoPool;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	private static final int MESSAGE_SIZE = 250000;

	public static final String DEFAULT_NAME = "largeMessageProxy";

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}
	
	private SortedSet<OrderedMessage> receivedChunks = new TreeSet<>(Comparator.comparing(OrderedMessage::getSerialNumber));

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class OrderedMessage implements Serializable {
		private byte[] bytes;
		private long serialNumber;
		private long endNumber;
	}

	/////////////////
	// Actor State //
	/////////////////

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// This will definitely fail in a distributed setting if the serialized message is large!
		// Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).

		ByteArrayOutputStream bOutput = new ByteArrayOutputStream(MESSAGE_SIZE);

		KryoPool kryo = KryoPoolSingleton.get();
		byte [] b = kryo.toBytesWithClass(message.getMessage());

		int end_number = (int) Math.ceil((double)b.length / MESSAGE_SIZE) - 1;

		for(int i = 0; i < b.length; i += MESSAGE_SIZE){
			byte [] chunk = kryo.toBytesWithoutClass(
					new OrderedMessage(Arrays.copyOfRange(b, i, i + MESSAGE_SIZE), i / MESSAGE_SIZE, end_number)
			);

			receiverProxy.tell(new BytesMessage<>(chunk, this.sender(), message.getReceiver()), this.self());
		}
	}

	private void handle(BytesMessage<?> message) {
		KryoPool kryo = KryoPoolSingleton.get();
		OrderedMessage m = kryo.fromBytes((byte[]) message.getBytes(), OrderedMessage.class);
		receivedChunks.add(m);

		if(m.serialNumber == m.endNumber) {
			ByteArrayOutputStream fullMessage = new ByteArrayOutputStream();

			for (OrderedMessage receivedChunk : receivedChunks) {
				try {
					fullMessage.write(receivedChunk.getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			message.getReceiver().tell(kryo.fromBytes(fullMessage.toByteArray()), message.getSender());
		}
	}
}
