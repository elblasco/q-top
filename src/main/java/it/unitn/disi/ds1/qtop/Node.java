package it.unitn.disi.ds1.qtop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static it.unitn.disi.ds1.qtop.Utils.*;

/**
 * Node class, it represents a single node in the network. It can be either receiver or coordinator.
 * During the election all the nodes become voters.
 */
public class Node extends AbstractActor {
	private static final int COUNTDOWN_REFRESH = 10;
	private static final Random rand = new Random();
	private final VotersMap voters = new VotersMap();
	private final TimeOutManager timeOutManager;
	private final int numberOfNodes;
	private final int writeTimeout;
	private final int electionGlobalTimeout;
	private final int nodeId;
	private Cancellable[] heartBeat;
	private List<ActorRef> group;
	private Utils.CrashType crashType = CrashType.NO_CRASH;
	private ActorRef coordinator;
	private Utils.Quadruplet lastElectionData;
	private PairsHistory history;
	private Utils.CrashType crashTypeToForward = CrashType.NO_CRASH;
	private int quorum;
	private boolean isElection = false;
	private int numbersOfWrites = 0;

	private final Logger logger = Logger.getInstance();

	public Node(ActorRef coordinator, int nodeId, int voteTimeout, int writeTimeout, int electionGlobalTimeout,
			int numberOfNodes) {
		this.history = new PairsHistory();
		this.nodeId = nodeId;
		this.writeTimeout = writeTimeout;
		this.electionGlobalTimeout = electionGlobalTimeout;
		this.numberOfNodes = numberOfNodes;
		this.coordinator = coordinator;
		System.out.println("The node received " + electionGlobalTimeout + " as global election timeout");
		this.timeOutManager = new TimeOutManager(
				voteTimeout,
				HEARTBEAT_TIMEOUT,
				writeTimeout,
				CRASH_TIMEOUT,
				electionGlobalTimeout,
				0,
				Node.COUNTDOWN_REFRESH
		);
		if (coordinator == null)
		{
			this.becomeCoordinator();
		}
	}

	static public Props props(ActorRef coordinator, int nodeId, int voteTimeout, int writeTimeout,
			int electionGlobalTimeout, int numberOfNodes) {
		return Props.create(
				Node.class,
				() -> new Node(
						coordinator,
						nodeId,
						voteTimeout,
						writeTimeout,
						electionGlobalTimeout,
						numberOfNodes
				)
		);
	}

	/**
	 * Generate the group of nodes.
	 *
	 * @param sm the start message
	 */
	private void setGroup(@NotNull StartMessage sm) {
		this.group = new ArrayList<>();
		this.group.addAll(sm.group());
	}

	/**
	 * Multicast a message to every node in the group in random order, it can make the sender crash.
	 *
	 * @param m          message to multicast
	 * @param hasToCrash if the current node has to crash
	 */
	private void multicast(Serializable m, boolean hasToCrash) {
		ArrayList<ActorRef> groupCopy = new ArrayList<>(this.group);
		Collections.shuffle(groupCopy);
		for (ActorRef node : groupCopy)
		{
			if (hasToCrash && (rand.nextInt(100) < 20) && node != this.getSelf())
			{
				this.coordinatorCrash();
				return;
			}
			this.tell(
					node,
					m,
					getSelf()
			);
		}
	}

	/**
	 * Make a vote, fix it and then send it back to the coordinator.
	 *
	 * @param msg request to make a vote
	 */
	private void onVoteRequest(VoteRequest msg) {
		if (this.crashType == CrashType.NODE_AFTER_VOTE_REQUEST)
		{
			logger.log(
					LogLevel.ERROR,
					"[NODE-" + this.nodeId + "] crashed!!! for reason NODE_AFTER_VOTE_REQUEST"
			);
			this.crash();
			return;
		}
		int e = msg.epoch().e();
		int i = msg.epoch().i();
		this.history.insert(
				e,
				i,
				msg.newValue()
		);
		Vote vote = new Random().nextBoolean() ? Vote.YES : Vote.NO;
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] sending vote " + vote + " for epoch < " + e + ", " + i + " > and new " +
						"proposed variable " + msg.newValue()
		);
		this.tell(
				this.getSender(),
				new VoteResponse(
						vote,
						new EpochPair(msg.epoch())
				),
				this.getSelf()
		);
		if (this.crashType == CrashType.NODE_AFTER_VOTE_CAST)
		{
			logger.log(
					LogLevel.ERROR,
					"[NODE-" + this.nodeId + "] crashed!!! for reason NODE_AFTER_VOTE_CAST"
			);
			this.crash();
		}
	}

	/**
	 * Handle the ReadRequest from a Client, if a crash for the coordinator is set it triggers it.
	 *
	 * @param msg the ReadRequest
	 */
	private void onReadRequest(ReadRequest msg) {
		this.tell(
				this.getSender(),
				new ReadValue(
						this.history.readValidVariable(),
						msg.nRequest()
				),
				this.getSelf()
		);
	}

	/**
	 * Make the current Node crash.
	 */
	private void crash() {
		logger.log(
				LogLevel.ERROR,
				"[NODE-" + this.nodeId + "] entered the crash state"
		);
		this.getContext().become(receiveBuilder().matchAny(msg -> {
		}).build());
	}

	/**
	 * Send a message to a destination actor with a random delay, within 0 and 29 milliseconds.
	 *
	 * @param dest   the destination actor
	 * @param msg    the message to send
	 * @param sender the sender actor
	 */
	public void tell(ActorRef dest, final Object msg, final ActorRef sender) {
		this.getContext().getSystem().scheduler().scheduleOnce(
				Duration.ofMillis(rand.nextInt(30)),
				dest,
				msg,
				this.getContext().getSystem().dispatcher(),
				sender
		);
	}

	/**
	 * Handle DecisionResponse from the coordinator.
	 *
	 * @param msg the massage with the decision
	 */
	private void onDecisionResponse(@NotNull DecisionResponse msg) {
		int e = msg.epoch().e();
		int i = msg.epoch().i();
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] received the decision " + msg.decision() + " for epoch < " + e + ", " + i + " >"
		);
		this.history.setState(
				e,
				i,
				msg.decision()
		);
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] decided " + msg.decision() + " on shared variable " + this.history.get(e)
						.get(i).first()
		);
	}

	/**
	 * Start the countdown for the Heartbeat.
	 */
	private void startHeartBeatCountDown() {
		this.timeOutManager.startCountDown(
				TimeOutReason.HEARTBEAT,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(HEARTBEAT_TIMEOUT / COUNTDOWN_REFRESH),
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

	/**
	 * Handle the Synchronisation messages typically triggered after an election.
	 *
	 * @param msg the Synchronisation message
	 */
	private void onSynchronisation(@NotNull Synchronisation msg) {
		this.becomeReceiver();
		this.coordinator = this.getSender();
		this.isElection = false;
		this.lastElectionData = null;
		this.history = new PairsHistory(msg.history());
		this.numbersOfWrites = 0;
		this.timeOutManager.endElectionState();
		this.startHeartBeatCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] received synchronisation message from coordinator, the new epoch is going" + " to be <" + msg.newEpochPair()
						.e() + ", " + msg.newEpochPair()
						.i() + ">" + "last value is now : " + this.history.readValidVariable()
		);
	}

	/**
	 * Make the current Node become a coordinator.
	 */
	private void becomeCoordinator() {
		this.getContext().become(coordinatorBehaviour());
	}

	/**
	 * Make the current Node become a voter for the election process.
	 */
	private void becomeVoter() {
		this.getContext().become(voterBehaviour());
	}

	/**
	 * Make the current Node become a receiver, aka, a normal replica.
	 */
	private void becomeReceiver() {
		this.getContext().become(createReceive());
	}

	/**
	 * Mask for to the voter actor.
	 *
	 * @return the Receive object
	 */
	private Receive voterBehaviour() {
		return receiveBuilder().match(
				CountDown.class,
				this::voterOnCountDown
		).match(
				CrashRequest.class,
				this::onCrashRequest
		).match(
				ReadRequest.class,
				this::onReadRequest
		).match(
				TimeOut.class,
				this::voterOnTimeOut
		).match(
				Election.class,
				this::onElection
		).match(
				ElectionACK.class,
				this::onElectionAck
		).match(
				Synchronisation.class,
				this::onSynchronisation
		).match(
				WriteRequest.class,
				msg -> this.tell(
						this.getSender(),
						new WriteValue(
								msg.newValue(),
								msg.nRequest()
						),
						this.getSelf()
				)
		).match(
				CrashACK.class,
				ack -> logger.log(
						LogLevel.INFO,
						"[NODE-" + this.nodeId + "] received crash ACK while in voting state"
				)
		).matchAny(msg -> logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] received " + msg.getClass().getSimpleName() + " while in voting " + "state"
		)).build();
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// BEGIN RECEIVER METHODS
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Mask for to the Receiver actor.
	 *
	 * @return the Receive object
	 */
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
				this::onHeartBeat
		).match(
				CountDown.class,
				this::onCountDown
		).match(
				CrashRequest.class,
				this::onCrashRequest
		).match(
				ReadRequest.class,
				this::onReadRequest
		).match(
				WriteRequest.class,
				this::onWriteRequest
		).match(
				WriteValue.class,
				this::onWriteValue
		).match(
				TimeOut.class,
				this::onTimeOut
		).match(
				Election.class,
				this::onElection
		).match(
				ElectionACK.class,
				this::onElectionAck
		).match(
				Synchronisation.class,
				this::onSynchronisation
		).match(
				CrashACK.class,
				ack -> logger.log(
						LogLevel.INFO,
						"[NODE-" + this.nodeId + "] received crash ACK from coordinator"
				)
		).matchAny(msg -> {
		}).build();
	}

	/**
	 * Initial set up for a Receiver, should be called whenever an ActorRef becomes a Receiver.
	 *
	 * @param msg the init message
	 */
	private void onStartMessage(StartMessage msg) {
		this.setGroup(msg);
		this.startHeartBeatCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] starting with " + this.group.size() + " peer(s)"
		);
	}

	/**
	 * Handler for the Heartbeat message.
	 *
	 * @param msg the Heartbeat message
	 */
	private void onHeartBeat(HeartBeat msg) {
		this.timeOutManager.resetCountDown(
				TimeOutReason.HEARTBEAT,
				0
		);
	}

	/**
	 * Receiver handler for the countdown which a Receiver can support
	 *
	 * @param msg Genre of countdown
	 */
	private void onCountDown(@NotNull CountDown msg) {
		int countDownIndex = (msg.reason() == TimeOutReason.HEARTBEAT) ? 0 : msg.epoch().i();
		this.timeOutManager.handleCountDown(
				msg.reason(),
				countDownIndex,
				this.getSelf()
		);
	}

	/**
	 * Receiver handler for write requests, if a receiver crash is set this function triggers it.
	 *
	 * @param msg the WriteRequest message
	 */
	private void onWriteRequest(@NotNull WriteRequest msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] received write request with value " + msg.newValue()
		);
		if (this.crashType == CrashType.NODE_BEFORE_WRITE_REQUEST)
		{
			this.crash();
			return;
		}
		this.tell(
				this.getSender(),
				new WriteValue(
						msg.newValue(),
						msg.nRequest()
				),
				this.getSelf()
		);
		this.tell(
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
				"[NODE-" + this.nodeId + "] sending write request number " + numbersOfWrites + " to coordinator, new " + "value proposed: " + msg.newValue()
		);
		numbersOfWrites++;
		if (this.crashType == CrashType.NODE_AFTER_WRITE_REQUEST)
		{
			this.crash();
		}
	}

	/**
	 * Receiver handler for the WriteResponse message.
	 *
	 * @param msg the WriteResponse message
	 */
	private void onWriteValue(@NotNull WriteValue msg) {
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] received write ACK for write request number " + msg.nRequest() + " with " + "value " + msg.value()
		);
		this.timeOutManager.resetCountDown(
				TimeOutReason.WRITE,
				msg.nRequest()
		);
	}

	/**
	 * Receiver general purpose handler for the timeout messages.
	 *
	 * @param msg timeout message
	 */
	private void onTimeOut(@NotNull TimeOut msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] timed out for reason : " + msg.reason()
		);
		switch (msg.reason())
		{
			case WRITE:
			case HEARTBEAT:
				this.startElection();
				break;
			default:
				break;
		}
	}

	/**
	 * Receiver handler for the Election message.
	 *
	 * @param msg the Election message
	 */
	private void onElection(@NotNull Election msg) {
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] received election message from [NODE-" + this.getSender() + "] with " +
						"params < e:" + msg.highestEpoch() + ", i:" + msg.highestIteration() + ">, best " + "candidate"
						+ " received:" + msg.bestCandidateId()
		);
		this.tell(
				this.getSender(),
				new ElectionACK(),
				this.getSelf()
		);
		int idDest = getNextNodeForElection();
		EpochPair nodeLatest = this.history.getLatestCommitted();
		if (this.isElection)
		{
			if (msg.bestCandidateId() == this.nodeId)
			{
				this.becomeCoordinator();
				this.isElection = false;
				this.timeOutManager.endElectionState();
				this.history.add(new ArrayList<>());
				this.numbersOfWrites = 0;
				logger.log(
						LogLevel.INFO,
						"[NODE-" + this.nodeId + "] elected as coordinator"
				);
				this.group.remove(this.coordinator);
				this.getSelf().tell(
						new StartMessage(this.group),
						this.getSelf()
				);
				this.multicast(
						new Synchronisation(
								new PairsHistory(this.history),
								new Utils.EpochPair(
										this.history.getLatestCommitted().e(),
										this.history.getLatestCommitted().i()
								)
						),
						false
				);
			}
			else if (msg.isGreaterThanLocalData(
					this.nodeId,
					nodeLatest
			))
			{
				this.forwardPreviousElectionMessage(
						msg,
						idDest
				);
			}
			else
			{
				logger.log(
						LogLevel.DEBUG,
						"[NODE-" + this.nodeId + "] is not going to forward election message from " + this.getSender()
				);
			}
		}
		else
		{
			this.startGlobalElectionCountDown();
			this.becomeVoter();
			this.timeOutManager.startElectionState();
			this.isElection = true;
			if (msg.isGreaterThanLocalData(
					this.nodeId,
					nodeLatest
			))
			{
				this.forwardPreviousElectionMessage(
						msg,
						idDest
				);
			}
			else
			{
				this.sendNewElectionMessage(
						nodeLatest,
						idDest
				);
			}
		}
	}

	/**
	 * Start the election process.
	 */
	private void startElection() {
		if (! this.isElection)
		{
			this.startGlobalElectionCountDown();
			this.becomeVoter();
			logger.log(
					LogLevel.INFO,
					"[NODE-" + this.nodeId + "] started the election process, my history is: < e:" + this.history.getLatestCommitted()
							.e() + ", i:" + this.history.getLatestCommitted().i() + " >"
			);
			this.isElection = true;
			int idDest = getNextNodeForElection();
			EpochPair latest = this.history.getLatestCommitted();
			this.lastElectionData = new Utils.Quadruplet(
					idDest,
					latest.e(),
					latest.i(),
					this.nodeId
			);
			this.timeOutManager.startElectionState();
			this.sendNewElectionMessage(
					latest,
					idDest
			);
		}
	}

	/**
	 * Handler for the ElectionACK message.
	 *
	 * @param msg the ElectionACK message
	 */
	private void onElectionAck(ElectionACK msg) {
		this.timeOutManager.resetCountDown(
				TimeOutReason.ELECTION,
				0
		);
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] received election ACK from [NODE-" + this.getSender() + "]"
		);
	}

	/**
	 * Get the next node to send the election message. It is called if the previous Node did not reply in time.
	 *
	 * @return the next node ID to send the election message
	 */
	private int getNextNodeForElection() {
		int idDest = (this.nodeId + 1) % this.numberOfNodes;
		while (this.group.get(idDest) == this.coordinator && idDest != this.nodeId)
		{
			idDest = (idDest + 1) % this.numberOfNodes;
		}
		return idDest;
	}

	/**
	 * Start an Election CountDown to wait the ElectionACK
	 */
	private void startElectionCountDown() {
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] starting election countdown"
		);
		this.timeOutManager.startCountDown(
				TimeOutReason.ELECTION,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(ELECTION_TIMEOUT / COUNTDOWN_REFRESH),
						getSelf(),
						new CountDown(
								TimeOutReason.ELECTION,
								null
						),
						getContext().getSystem().dispatcher(),
						getSelf()
				),
				0
		);
	}

	private void startGlobalElectionCountDown() {
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] starting global election countdown"
		);
		this.timeOutManager.startCountDown(
				TimeOutReason.ELECTION_GLOBAL,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(this.electionGlobalTimeout / COUNTDOWN_REFRESH),
						getSelf(),
						new CountDown(
								TimeOutReason.ELECTION_GLOBAL,
								null
						),
						getContext().getSystem().dispatcher(),
						getSelf()
				),
				0
		);
	}

	/**
	 * Retry the election process if the previous Node did not reply.
	 */
	private void retryElection() {
		int oldId = this.lastElectionData.destinationId();
		int newDestId = (oldId + 1) % numberOfNodes;
		logger.log(
				LogLevel.DEBUG,
				getSelf() + " is retrying election sending to [NODE-" + newDestId + "] the best " + "candidate " + this.lastElectionData.bestCandidateId()
		);
		this.lastElectionData = new Quadruplet(
				newDestId,
				this.lastElectionData.highestEpoch(),
				this.lastElectionData.highestIteration(),
				this.lastElectionData.bestCandidateId()
		);
		this.forwardPreviousElectionMessage(
				new Election(
						this.lastElectionData.highestEpoch(),
						this.lastElectionData.highestIteration(),
						this.lastElectionData.bestCandidateId()
				),
				newDestId
		);
	}

	/**
	 * Start the Write CountDown to wait the WriteResponse
	 */
	private void startWriteCountDown() {
		this.timeOutManager.startCountDown(
				TimeOutReason.WRITE,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(this.writeTimeout / COUNTDOWN_REFRESH),
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

	/**
	 * Forward the previous election message to the next node. It is used only if the current Node will lose the
	 * election.
	 *
	 * @param msg    the Election message
	 * @param idDest the destination node ID
	 */
	private void forwardPreviousElectionMessage(@NotNull Election msg, int idDest) {
		this.startElectionCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] forwarding election message with best candidate " + msg.bestCandidateId()
		);
		this.lastElectionData = new Utils.Quadruplet(
				idDest,
				msg.highestEpoch(),
				msg.highestIteration(),
				msg.bestCandidateId()
		);
		this.tell(
				this.group.get(idDest),
				msg,
				this.getSelf()
		);
	}

	/**
	 * Craft a new Election message and send it to the next node.
	 *
	 * @param highestData the highest epoch and iteration registered by the current Node
	 * @param idDest      the destination node ID
	 */
	private void sendNewElectionMessage(@NotNull EpochPair highestData, int idDest) {
		this.startElectionCountDown();
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] switched election state, my history is < e:" + highestData.e() + ", i:" + highestData.i() + ">, sending new election message to [NODE-" + idDest + "]"
		);
		this.lastElectionData = new Utils.Quadruplet(
				idDest,
				highestData.e(),
				highestData.i(),
				this.nodeId
		);
		this.tell(
				this.group.get(idDest),
				new Election(
						highestData.e(),
						highestData.i(),
						this.nodeId
				),
				this.getSelf()
		);
	}

	/**
	 * Handle the CrashRequest messages. In case it is a message for the coordinator it forwards the message.
	 *
	 * @param msg the CrashRequest message
	 */
	private void onCrashRequest(@NotNull CrashRequest msg) {
		getSender().tell(
				new CrashACK(),
				getSelf()
		);
		switch (msg.crashType())
		{
			case NODE_BEFORE_WRITE_REQUEST:
			case NODE_AFTER_WRITE_REQUEST:
			case NODE_AFTER_VOTE_REQUEST:
			case NODE_AFTER_VOTE_CAST:
				this.crashType = msg.crashType();
				logger.log(
						LogLevel.INFO,
						"[NODE-" + this.nodeId + "] will eventually crash because " + this.crashType
				);
				break;
			case NO_CRASH:
				break;
			default:
				// The crash are forwarded without delay
				this.coordinator.tell(
						msg,
						getSelf()
				);
				break;
		}
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// BEGIN COORDINATOR METHODS
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Mask for to the Coordinator actor.
	 *
	 * @return the Receive object
	 */
	private AbstractActor.Receive coordinatorBehaviour() {
		return receiveBuilder().match(
				StartMessage.class,
				this::coordinatorOnStartMessage
		).match(
				VoteResponse.class,
				this::coordinatorOnVoteResponse
		).match(
				VoteRequest.class,
				this::onVoteRequest
		).match(
				ReadRequest.class,
				this::onReadRequest
		).match(
				DecisionResponse.class,
				this::onDecisionResponse
		).match(
				HeartBeat.class,
				//Special handler for the self sent Heartbeat, the print is just for debug
				heartBeat -> logger.log(
						LogLevel.DEBUG,
						"[NODE-" + this.nodeId + "] [Coordinator] heartbeat"
				)
		).match(
				CountDown.class,
				this::coordinatorOnCountDown
		).match(
				CrashRequest.class,
				this::coordinatorOnCrashRequest
		).match(
				WriteRequest.class,
				this::coordinatorOnWriteRequest
		).match(
				TimeOut.class,
				this::coordinatorOnTimeOut
		).match(
				CrashACK.class,
				this::coordinatorOnCrashACK
		).match(
				Synchronisation.class,
				msg -> System.out.println("[NODE-" + this.nodeId + "] elected as new leader")
		).matchAny(msg -> {
		}).build();

	}

	/**
	 * Initial set up for the Coordinator, should be called whenever an ActorRef becomes Coordinator.
	 *
	 * @param msg the init message
	 */
	private void coordinatorOnStartMessage(StartMessage msg) {
		this.heartBeat = new Cancellable[numberOfNodes];
		this.quorum = (numberOfNodes / 2) + 1;
		this.setGroup(msg);
		this.startHeartBeat();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] [Coordinator] starting with " + this.group.size() + " peer(s)"
		);
	}

	/**
	 * Register a Node vote.
	 * Then, if the quorum is reached or everybody voted, fix the decision and multicast the decision.
	 *
	 * @param msg request to make a vote
	 */
	private void coordinatorOnVoteResponse(@NotNull VoteResponse msg) {
		Vote v = msg.vote();
		int e = msg.epoch().e();
		int i = msg.epoch().i();
		voters.insert(
				e,
				i,
				this.getSender(),
				v
		);
		boolean isQuorumReached = quorumReached(
				e,
				i
		);
		if ((isQuorumReached || voters.get(e).get(i).votes().size() == this.numberOfNodes) && this.voters.get(e).get(i)
				.finalDecision() == Decision.PENDING)
		{
			fixCoordinatorDecision(
					isQuorumReached ? Decision.WRITEOK : Decision.ABORT,
					e,
					i
			);
			multicast(
					new DecisionResponse(
							this.voters.get(e).get(i).finalDecision(),
							new EpochPair(msg.epoch())
					),
					this.crashType == CrashType.COORDINATOR_ON_DECISION_RESPONSE
			);
			this.history.setState(
					e,
					i,
					this.voters.get(e).get(i).finalDecision()
			);
		}
	}

	/**
	 * Coordinator handler for the WriteRequest messages. If a crash for the coordinator is set it triggers it.
	 *
	 * @param msg the WriteRequest message
	 */
	private void coordinatorOnWriteRequest(WriteRequest msg) {
		this.tell(
				this.getSender(),
				new WriteValue(
						msg.newValue(),
						msg.nRequest()
				),
				this.getSelf()
		);
		int e = this.history.isEmpty() ? 0 : this.history.size() - 1;
		int i = (this.history.isEmpty() || this.history.get(e).isEmpty()) ? 0 : this.history.get(e).size();
		this.history.insert(
				e,
				i,
				msg.newValue()
		);
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] [Coordinator] sending write response for write request number " + msg.nRequest() + " with value " + msg.newValue()
		);
		multicast(
				new VoteRequest(
						msg.newValue(),
						new EpochPair(
								e,
								i
						)
				),
				this.crashType == CrashType.COORDINATOR_ON_VOTE_REQUEST
		);
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] [Coordinator] Sent vote request to write " + msg.newValue() + " for epoch "
						+ "< " + e + ", " + i + " >"
		);
	}

	/**
	 * Coordinator general purpose handler for countdowns.
	 *
	 * @param msg the CountDown message
	 */
	private void coordinatorOnCountDown(@NotNull CountDown msg) {
		this.timeOutManager.handleCountDown(
				msg.reason(),
				0,
				this.getSelf()
		);
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] [Coordinator] countdown of type " + msg.reason()
		);
	}

	/**
	 * Check if the quorum is reached for a specific epoch and iteration.
	 *
	 * @param e the epoch
	 * @param i the iteration
	 *
	 * @return true if the quorum is reached, false otherwise
	 */
	private boolean quorumReached(int e, int i) {
		return voters.get(e).get(i).votes().entrySet().stream().filter(entry -> entry.getValue() == Vote.YES).toList()
				.size() >= quorum;
	}

	/**
	 * Fix the Coordinator decision.
	 *
	 * @param d decision took by the coordinator
	 */
	private void fixCoordinatorDecision(Decision d, int e, int i) {
		if (this.voters.get(e).get(i).finalDecision() == Decision.PENDING)
		{
			this.voters.setDecision(
					d,
					e,
					i
			);
			logger.log(
					LogLevel.INFO,
					"[NODE-" + this.nodeId + "] [Coordinator] collected enough votes, decided and broadcasting " + d + " " + "for epoch < " + e + ", " + i + " >"
			);
		}
	}

	/**
	 * Assign to every node in the group a Heartbeat scheduled message
	 */
	private void startHeartBeat() {
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "] [Coordinator] starting heartbeat protocol"
		);
		// Yes the coordinator sends a Heartbeat to itself
		for (int i = 0; i < this.numberOfNodes; ++ i)
		{
			heartBeat[i] = getContext().getSystem().scheduler().scheduleAtFixedRate(
					Duration.ZERO,
					Duration.ofMillis(HEARTBEAT_TIMEOUT / COUNTDOWN_REFRESH),
					this.group.get(i),
					new HeartBeat(),
					getContext().getSystem().dispatcher(),
					getSelf()
			);
		}
	}

	/**
	 * Coordinator handler for the CrashRequest messages. If a message is for a receiver it forwards the message.
	 *
	 * @param msg the CrashRequest message
	 */
	private void coordinatorOnCrashRequest(CrashRequest msg) {
		if (getSender() != getSelf())
		{
			getSender().tell(
					new CrashACK(),
					getSelf()
			);
		}
		switch (msg.crashType())
		{
			case COORDINATOR_ON_VOTE_REQUEST:
			case COORDINATOR_ON_DECISION_RESPONSE:
				this.crashType = msg.crashType();
				logger.log(
						LogLevel.INFO,
						"[NODE-" + this.nodeId + "] [COORDINATOR] will eventually crash because " + this.crashType
				);
				break;
			case NO_CRASH:
				break;
			default:
				this.crashTypeToForward = msg.crashType();
				this.timeOutManager.startCountDown(
						TimeOutReason.CRASH_RESPONSE,
						this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
								Duration.ZERO,
								Duration.ofMillis(CRASH_TIMEOUT / COUNTDOWN_REFRESH),
								getSelf(),
								new CountDown(
										TimeOutReason.CRASH_RESPONSE,
										null
								),
								getContext().getSystem().dispatcher(),
								getSelf()
						),
						0
				);
				this.group.get(rand.nextInt(this.numberOfNodes)).tell(
						msg,
						getSelf()
				);
				break;
		}
	}

	/**
	 * Coordinator general purpose handler for the timeout messages.
	 *
	 * @param msg the TimeOut message
	 */
	private void coordinatorOnTimeOut(@NotNull TimeOut msg) {
		if (msg.reason() == TimeOutReason.CRASH_RESPONSE)
		{
			this.timeOutManager.resetCountDown(
					TimeOutReason.CRASH_RESPONSE,
					0
			);
			getSelf().tell(
					new CrashRequest(this.crashTypeToForward),
					getSelf()
			);
		}
	}

	/**
	 * Coordinator handler for the CrashACK messages.
	 *
	 * @param msg the CrashACK message
	 */
	private void coordinatorOnCrashACK(CrashACK msg) {
		this.timeOutManager.resetCountDown(
				TimeOutReason.CRASH_RESPONSE,
				0
		);
		this.crashTypeToForward = null;
	}

	/**
	 * Coordinator wrapper for the crash method.
	 */
	private void coordinatorCrash() {
		if (this.heartBeat != null)
		{
			for (Cancellable heart : this.heartBeat)
			{
				heart.cancel();
			}
		}
		this.crash();
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// BEGIN VOTERS METHODS
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private void voterOnTimeOut(TimeOut msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] timed out for reason : " + msg.reason()
		);
		switch (msg.reason())
		{
			case ELECTION:
				this.retryElection();
				break;
			case ELECTION_GLOBAL:
				this.isElection = false;
				this.lastElectionData = null;
				this.timeOutManager.endElectionState();
				this.startElection();
				break;
			default:
				break;
		}
	}

	/**
	 * Voter handler for the countdown which a Receiver can support
	 *
	 * @param msg Genre of countdown
	 */
	private void voterOnCountDown(@NotNull CountDown msg) {
		int countDownIndex =
				(msg.reason() == TimeOutReason.ELECTION || msg.reason() == TimeOutReason.ELECTION_GLOBAL) ? 0 :
						msg.epoch()
						.i();
		this.timeOutManager.handleCountDown(
				msg.reason(),
				countDownIndex,
				this.getSelf()
		);
	}
}