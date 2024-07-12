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
	private Utils.Triple<Integer, Integer, Integer> lastElectionData;

	private final Logger logger = Logger.getInstance();

	public Receiver(int nodeId, ActorRef coordinator, int numberOfNodes, int decisionTimeout, int voteTimeout,
			int writeTimeout) {
		super(
				nodeId,
				decisionTimeout,
				voteTimeout,
				writeTimeout
		);
		super.setNumberOfNodes(numberOfNodes);
		this.coordinator = coordinator;
	}

	static public Props props(int nodeId, ActorRef coordinator, int numberOfNodes, int decisionTimeout,
			int voteTimeout,
			int writeTimeout) {
		return Props.create(
				Receiver.class,
				() -> new Receiver(
						nodeId,
						coordinator,
						numberOfNodes,
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
		super.timeOutManager.deleteCountDown(
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
		super.timeOutManager.handleCountDown(
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
		super.timeOutManager.deleteCountDown(
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
				this.startElection();
				break;
			case ELECTION:
				// TODO election timeout handling, new next node computation and message sending
				break;
			default:
				break;
		}
	}

	private void startElection() {
		if (! this.isElection)
		{
			logger.log(
					LogLevel.INFO,
					"[NODE-" + super.nodeId + "] started the election process"
			);
			this.isElection = true;
			int idDest = getNextNodeForElection();
			Pair<Integer, Integer> latest = super.getHistory().getLatest();
			this.lastElectionData = new Utils.Triple<>(
					idDest,
					latest.first(),
					latest.second()
			);
			super.timeOutManager.startElectionState();
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
	}

	private int getNextNodeForElection() {
		int idDest = (super.nodeId + 1) % super.getNumberOfNodes();
		while (super.group.get(idDest) == this.coordinator)
		{
			idDest = (idDest + 1) % super.getNumberOfNodes();
		}
		return idDest;
	}

	private void startElectionCountDown() {
		super.timeOutManager.startCountDown(
				TimeOutReason.ELECTION,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(ELECTION_TIMEOUT / COUNTDOWN_REFRESH),
						getSelf(),
						new CountDown(
								TimeOutReason.ELECTION,
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

	private void onElectionAck(ElectionACK msg) {
		super.timeOutManager.deleteCountDown(
				TimeOutReason.ELECTION,
				0,
				super.nodeId,
				logger
		);
	}

	private void startWriteCountDown() {
		super.timeOutManager.startCountDown(
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

	private void onElection(Election msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + super.nodeId + "] received election message from [NODE-" + this.getSender() + "]"
		);
		super.tell(
				this.getSender(),
				new ElectionACK(),
				this.getSelf()
		);
		int idDest = getNextNodeForElection();
		Pair<Integer, Integer> nodeLatest = super.getHistory().getLatest();
		if (this.isElection)
		{
			if (msg.bestCandidateId() == super.nodeId)
			{
				// TODO leader decided (It is me) multicast the SYNCHRONIZATION and upgrade to coordinator
				this.lastElectionData = null;
				logger.log(
						LogLevel.INFO,
						"[NODE-" + super.nodeId + "] elected as supreme leader"
				);
			}
			else if (msg.highestIteration() > nodeLatest.second() && msg.highestEpoch() > nodeLatest.first())
			{
				this.forwardPreviousElectionMessage(
						msg,
						idDest
				);
			}
		}
		else
		{
			this.isElection = true;
			if (msg.highestIteration() > nodeLatest.second() && msg.highestEpoch() > nodeLatest.first())
			{
				this.forwardPreviousElectionMessage(
						msg,
						idDest
				);
			}
			else
			{
				this.ForwardNewElectionMessage(
						nodeLatest,
						idDest
				);
			}
		}
		this.startElectionCountDown();
	}

	private void forwardPreviousElectionMessage(Election msg, int idDest) {
		this.lastElectionData = new Utils.Triple<>(
				idDest,
				msg.highestEpoch(),
				msg.highestIteration()
		);
		super.tell(
				super.group.get(idDest),
				msg,
				this.getSelf()
		);
	}

	private void ForwardNewElectionMessage(Pair<Integer, Integer> highestData, int idDest) {
		this.lastElectionData = new Utils.Triple<>(
				idDest,
				highestData.first(),
				highestData.second()
		);
		super.tell(
				super.group.get(idDest),
				new Election(
						highestData.first(),
						highestData.second(),
						super.nodeId
				),
				this.getSelf()
		);
	}
}