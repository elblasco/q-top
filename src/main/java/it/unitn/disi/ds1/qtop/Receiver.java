package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Pair;

import java.time.Duration;

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Receiver extends Node {
	private ActorRef coordinator;
	private boolean isElection = false;
	private int numbersOfWrites = 0;

	private final Logger logger = Logger.getInstance();

	public Receiver(int nodeId, ActorRef coordinator, int decisionTimeout, int voteTimeout, int writeTimeout) {
		super(
				nodeId,
				decisionTimeout,
				voteTimeout,
				writeTimeout
		);
		this.coordinator = coordinator;
	}

	static public Props props(int nodeId, ActorRef coordinator, int decisionTimeout, int voteTimeout,
			int writeTimeout) {
		return Props.create(
				Receiver.class,
				() -> new Receiver(
						nodeId,
						coordinator,
						decisionTimeout,
						voteTimeout,
						writeTimeout
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
		).match(
				WriteResponse.class,
				this::onWriteResponse
		).match(
				TimeOut.class,
				this::onTimeOut
		).match(
				Election.class,
				this::onElection
		).match(
				ElectionACK.class,
				this::onElectionAck
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
		super.startHeartBeatCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + super.nodeId + "] starting with " + super.group.size() + " peer(s)"
		);
	}

	private void onHeartBeat(HeartBeat msg) {
		super.timeouts.deleteCountDown(
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
		int countDownIndex =
				(msg.reason() == TimeOutReason.HEARTBEAT || msg.reason() == TimeOutReason.ELECTION) ? 0 : msg.epoch()
						.i();
		super.timeouts.handleCountDown(
				msg.reason(),
				countDownIndex,
				this,
				this.logger
		);
	}

	private void onWriteRequest(WriteRequest msg) {
		super.tell(
				this.coordinator,
				new WriteRequest(
						msg.newValue(),
						numbersOfWrites
				),
				this.getSelf()
		);
		this.startWriteCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + super.nodeId + "] sending write request number " + numbersOfWrites + " with value " + msg.newValue()
		);
		numbersOfWrites++;
	}

	private void onWriteResponse(WriteResponse msg) {
		super.timeouts.deleteCountDown(
				TimeOutReason.WRITE,
				msg.nRequest(),
				nodeId,
				this.logger
		);
	}

	private void onTimeOut(TimeOut msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + super.nodeId + "] received a timeout " + msg.reason()
		);
		switch (msg.reason())
		{
			case WRITE:
			case HEARTBEAT:
				this.startLeaderElection();
				break;
			default:
				break;
		}
	}

	private void onElection(Election msg) {
		super.tell(
				this.getSender(),
				new ElectionACK(),
				this.getSelf()
		);
		int idDest = (super.nodeId + 1) % super.getNumberOfNodes();
		Pair<Integer, Integer> nodeLatest = super.getHistory().getLatest();
		if (this.isElection)
		{
			if (msg.bestCandidateId() == super.nodeId)
			{
				// TODO leader decided (It is me) multicast the SYNCHRONIZATION and upgrade to coordinator
				System.out.println("I am the leader!!!!!!");
			}
			else if (msg.highestIteration() > nodeLatest.second() && msg.highestEpoch() > nodeLatest.first())
			{
				super.tell(
						super.group.get(idDest),
						msg,
						this.getSelf()
				);
			}
		}
		else
		{
			this.isElection = true;
			if (msg.highestIteration() > nodeLatest.second() && msg.highestEpoch() > nodeLatest.first())
			{
				super.tell(
						super.group.get(idDest),
						msg,
						this.getSelf()
				);
			}
			else
			{
				super.tell(
						super.group.get(idDest),
						new Election(
								nodeLatest.first(),
								nodeLatest.second(),
								super.nodeId
						),
						this.getSelf()
				);
			}
		}
		this.startElectionCountDown();
	}

	private void onElectionAck(ElectionACK msg) {
		super.timeouts.deleteCountDown(
				TimeOutReason.ELECTION,
				0,
				super.nodeId,
				logger
		);
	}

	private void startWriteCountDown() {
		super.timeouts.startCountDown(
				TimeOutReason.WRITE,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(super.writeTimeout / COUNTDOWN_REFRESH),
						getSelf(),
						new CountDown(
								TimeOutReason.WRITE,
								new EpochPair(
										0,
										this.numbersOfWrites
								)
						),
						getContext().getSystem().dispatcher(),
						getSelf()
				),
				this.numbersOfWrites
		);
	}

	private void startLeaderElection() {
		this.isElection = true;
		int idDest = (super.nodeId + 1) % super.getNumberOfNodes();
		Pair<Integer, Integer> latest = super.getHistory().getLatest();
		this.startElectionCountDown();
		super.tell(
				super.group.get(idDest),
				new Election(
						latest.first(),
						latest.second(),
						super.nodeId
				),
				this.getSelf()
		);
	}

	private void startElectionCountDown() {
		super.timeouts.startCountDown(
				TimeOutReason.ELECTION,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(ELECTION_TIMEOUT / COUNTDOWN_REFRESH),
						getSelf(),
						new CountDown(
								TimeOutReason.HEARTBEAT,
								new EpochPair(
										0,
										0
								)
						),
						getContext().getSystem().dispatcher(),
						getSelf()
				),
				0
		);
	}
}