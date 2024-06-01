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

	private final Logger logger = Logger.getInstance();

    public Receiver(int nodeId, ActorRef coordinator, int decisionTimeout, int voteTimeout) {
        super(nodeId);
        this.coordinator = coordinator;

    }

    static public Props props(int nodeId, ActorRef coordinator, int decisionTimeout, int voteTimeout) {
        return Props.create(Receiver.class,() -> new Receiver(nodeId, coordinator, decisionTimeout, voteTimeout));
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
				//Reset the countdown
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
		logger.log(LogLevel.INFO,"[NODE-"+this.nodeId+"] starting with " + this.group.size() + " peer(s)");
	}

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     * @param msg request to make a vote
     */
    protected void onVoteRequest(VoteRequest msg) {
        super.onVoteRequest(msg);
        this.coordinator.tell(new VoteResponse(this.nodeVote), getSelf());
    }

	/**
	 * General purpose handler for the countdown which a Receiver can support
	 *
	 * @param msg Genre of countdown
	 */
	private void onCountDown(CountDown msg) {
		switch (msg.reason())
		{
			case HEARTBEAT -> handleHeartBeatCountDown();
			default -> System.out.println("CountDown reason not handled by coordinator " + this.nodeId);
		}
	}

	/**
	 * Handler for Heartbeat countdown, it can cancel the countdown schedule.
	 */
	private void handleHeartBeatCountDown() {
		if (this.heartBeatCountdown <= 0)
		{
			heartBeatCountdownTimer.cancel();
			logger.log(LogLevel.INFO,"[NODE-"+this.nodeId+"]  notified with and heartbeat timeout, timer is " + this.heartBeatCountdown);
		}
		else
		{
			logger.log(LogLevel.INFO,"[NODE-"+this.nodeId+"] notified with and heartbeat countdown");
			this.heartBeatCountdown -= HEARTBEAT_TIMEOUT / HEARTBEAT_COUNTDOWN_REFRESH;
		}
	}

	/**
	 * Generate a scheduled message to check update the Heartbeat timeout counter.
	 */
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
}
