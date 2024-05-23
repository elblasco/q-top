package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.time.Duration;

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Receiver extends Node{
	private static final int HEARTBEAT_COUNTDOWN_REFRESH = 10; // times, within a second, the heartBeatCountdown is
	// updated
    private ActorRef coordinator;
	private int heartBeatCountdown = HEARTBEAT_TIMEOUT;
	private Cancellable heartBeatCountdownTimer;

    public Receiver(int nodeId, ActorRef coordinator) {
        super(nodeId);
        this.coordinator = coordinator;
    }

    static public Props props(int nodeId, ActorRef coordinator) {
        return Props.create(Receiver.class,() -> new Receiver(nodeId, coordinator));
    }

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(
				StartMessage.class,
				this::onStartMessage
		).match(
				VoteRequest.class,
				this::onVoteRequest
		).match(
				DecisionResponse.class,
				this::onDecisionResponse
		).match(
				HeartBeat.class,
				heartBeatCountdown -> this.heartBeatCountdown = HEARTBEAT_TIMEOUT
		).match(
				CountDown.class,
				this::onCountDown
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
		System.out.println(this.nodeId + " received a start message");
	}

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     * @param msg request to make a vote
     */
    protected void onVoteRequest(VoteRequest msg) {
        super.onVoteRequest(msg);
        this.coordinator.tell(new VoteResponse(this.nodeVote), getSelf());
    }

	private void onCountDown(CountDown msg) {
		switch (msg.reason())
		{
			case HEARTBEAT -> handleHeartBeatCountDown();
			default -> System.out.println("CountDown reason not handled by coordinator " + this.nodeId);
		}
	}

	private void startHeartBeatCountDown() {
		this.heartBeatCountdownTimer = getContext().getSystem().scheduler().scheduleAtFixedRate(
				Duration.ZERO,
				Duration.ofMillis(HEARTBEAT_TIMEOUT / HEARTBEAT_COUNTDOWN_REFRESH),
				getSelf(),
				new CountDown(TimeOutAndTickReason.HEARTBEAT),
				getContext().getSystem().dispatcher(),
				getSelf()
		);
	}

	private void handleHeartBeatCountDown() {
		if (this.heartBeatCountdown <= 0)
		{
			heartBeatCountdownTimer.cancel();
			System.out.println("Node " + this.nodeId + " notified with and heartbeat timeout, timer is " + this.heartBeatCountdown);
		}
		else
		{
			System.out.println("Node " + this.nodeId + " notified with and heartbeat countdown");
			this.heartBeatCountdown -= HEARTBEAT_TIMEOUT / HEARTBEAT_COUNTDOWN_REFRESH;
		}
	}
}
