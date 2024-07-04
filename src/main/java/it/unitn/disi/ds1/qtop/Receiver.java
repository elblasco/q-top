package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.Props;

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Receiver extends Node {
	private ActorRef coordinator;

	private final Logger logger = Logger.getInstance();

	public Receiver(int nodeId, ActorRef coordinator, int decisionTimeout, int voteTimeout) {
		super(
				nodeId,
				decisionTimeout,
				voteTimeout
		);
		this.coordinator = coordinator;
	}

	static public Props props(int nodeId, ActorRef coordinator, int decisionTimeout, int voteTimeout) {
		return Props.create(
				Receiver.class,
				() -> new Receiver(
						nodeId,
						coordinator,
						decisionTimeout,
						voteTimeout
				)
		);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(
				StartMessage.class,
				this::onStartMessage
		).match(
				VoteRequest.class,
				super::onVoteRequest
		).match(
				DecisionResponse.class,
				super::onDecisionResponse
		).match(
				HeartBeat.class,
				//Reset the countdown
				this::onHeartBeat
		).match(
				CountDown.class,
				this::onCountDown
		).match(
				CrashRequest.class,
				super::onCrashRequest
		).match(
				ReadRequest.class,
				super::onReadRequest
		).match(
				WriteRequest.class,
				this::onWriteRequest
		).build();
	}

	/**
	 * Initial set up for a Receiver, should be called whenever an ActorRef becomes a Receiver.
	 *
	 * @param msg the init message
	 */
	@Override
	protected void onStartMessage(StartMessage msg) {
		super.onStartMessage(msg);
		this.startHeartBeatCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + super.nodeId + "] starting with " + super.group.size() + " peer(s)"
		);
	}

	private void onHeartBeat(HeartBeat msg) {
		super.timeouts.resetCountDown(
				TimeOutReason.HEARTBEAT,
				0,
				nodeId,
				this.logger
		);
	}

	/**
	 * General purpose handler for the countdown which a Receiver can support
	 *
	 * @param msg Genre of countdown
	 */
	private void onCountDown(CountDown msg) {
		switch (msg.reason())
		{
			case HEARTBEAT -> super.timeouts.handleCountDown(
					TimeOutReason.HEARTBEAT,
					0,
					super.nodeId,
					this.logger
			);
			case DECISION -> super.timeouts.handleCountDown(
					TimeOutReason.DECISION,
					msg.epoch().i(),
					super.nodeId,
					this.logger
			);
			default -> System.out.println("CountDown reason not handled by coordinator " + this.nodeId);
		}
	}

	private void onWriteRequest(WriteRequest msg) {
		this.coordinator.tell(
				msg,
				this.getSelf()
		);
	}
}