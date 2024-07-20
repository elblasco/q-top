package it.unitn.disi.ds1.qtop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Node extends AbstractActor {
	private static final int COUNTDOWN_REFRESH = 10;
	private static final Random rand = new Random();
	private final int writeTimeout;
	private final int nodeId;
	private int quorum;
	private VotersMap voters = new VotersMap();
	private Cancellable[] heartBeat;
	private List<ActorRef> group;
	//private EpochPair epochPair;
    private final int decisionTimeout;
	private Utils.CrashType crashType = CrashType.NO_CRASH;
	private boolean crashed = false;
    private final int voteTimeout;
	private TimeOutManager timeOutManager;
	private ActorRef coordinator;
	private boolean isElection = false;
	private int numbersOfWrites = 0;
	private Utils.Quadruplet<Integer, Integer, Integer> lastElectionData;
    private PairsHistory history;
    private int numberOfNodes;

    private final Logger logger = Logger.getInstance();

	public Node(ActorRef coordinator, int nodeId, int decisionTimeout, int voteTimeout, int writeTimeout,
			int numberOfNodes) {
        super();
        this.history = new PairsHistory();
        this.nodeId = nodeId;
        this.decisionTimeout = decisionTimeout;
        this.voteTimeout = voteTimeout;
        this.writeTimeout = writeTimeout;
		this.numberOfNodes = numberOfNodes;
		this.coordinator = coordinator;
		/*this.epochPair = new EpochPair(
				0,
				0
		);*/
        this.timeOutManager = new TimeOutManager(
                decisionTimeout,
                voteTimeout,
                HEARTBEAT_TIMEOUT,
                writeTimeout,
                Node.COUNTDOWN_REFRESH
        );
		if (coordinator == null)
		{
			this.becomeCoordinator();
		}
    }

	static public Props props(ActorRef coordinator, int nodeId, int decisionTimeout, int voteTimeout, int writeTimeout,
			int numberOfNodes) {
		return Props.create(
				Node.class,
				() -> new Node(
						coordinator,
						nodeId,
						decisionTimeout,
						voteTimeout,
						writeTimeout,
						numberOfNodes
				)
		);
	}

	public int getNodeId() {
		return nodeId;
	}

	private void setGroup(StartMessage sm) {
        this.group = new ArrayList<>();
        this.group.addAll(sm.group());
    }

	private void multicast(Serializable m) {
        for (ActorRef node : group)
        {
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
            this.crash();
        }
	    this.history.insert(
                msg.epoch().e(),
                msg.epoch().i(),
                msg.newValue()
        );
        Vote vote = new Random().nextBoolean() ? Vote.YES : Vote.NO;
        logger.log(
                LogLevel.INFO,
                "[NODE-" + this.nodeId + "] sending vote " + vote + " for epoch < " + msg.epoch()
		                .e() + ", " + msg.epoch()
		                .i() + " > and variable " + msg.newValue() + "\nMy election state is " + this.isElection
        );
        this.tell(
                this.getSender(),
                new VoteResponse(
                        vote,
                        msg.epoch()
                ),
                this.getSelf()
        );
        if (this.crashType == CrashType.NODE_AFTER_VOTE_CAST)
        {
            this.crash();
        }
    }

	private void onCrashRequest(CrashRequest msg) {
        this.crashType = msg.crashType();
		logger.log(LogLevel.INFO, "[NODE-" + this.nodeId+ "] crashed because " + this.crashType);
    }

	private void onReadRequest(ReadRequest msg) {
		this.tell(
				this.getSender(),
				new ReadValue(this.history.readValidVariable()),
                this.getSelf()
        );
    }

	private void crash() {
        this.crashed = true;
        this.getContext().become(receiveBuilder().matchAny(msg -> {
        }).build());
    }

    public void tell(ActorRef dest, final Object msg, final ActorRef sender) {
	    try
	    {
		    Thread.sleep(rand.nextInt(10));
	    } catch (InterruptedException e)
	    {
		    e.printStackTrace();
	    }
        dest.tell(
                msg,
                sender
        );
    }

	private void onDecisionResponse(DecisionResponse msg) {
        int e = msg.epoch().e();
        int i = msg.epoch().i();
        logger.log(
                LogLevel.INFO,
                "[NODE-" + this.nodeId + "] received the decision " + msg.decision() + " for epoch < " + msg.epoch()
                        .e() + ", " + msg.epoch().i() + " >"
        );
        if (msg.decision() == Decision.WRITEOK)
        {
	        this.history.setStateToTrue(
                    e,
                    i
            );
            logger.log(
                    LogLevel.INFO,
		            "[NODE-" + this.nodeId + "] committed shared variable " + this.history.get(e).get(i)
				            .first() + " now the history is\n" + this.history
            );
        }
    }

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

	private void onSynchronisation(Synchronisation msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] received synchronisation message, the epoch is going to be <" + msg.newEpochPair()
						.e() + ", " + msg.newEpochPair().i() + ">"
		);
		this.coordinator = this.getSender();
		this.isElection = false;
		this.lastElectionData = null;
		this.history = msg.history();
		//this.epochPair = msg.newEpochPair();
		this.timeOutManager.endElectionState();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] has now last valid value: " + this.history.readValidVariable() + " with " + "history\n" + this.history
		);
	}

	public void becomeCoordinator() {
		this.getContext().become(coordinatorBehaviour());
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// BEGIN RECEIVER METHODS
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
		).match(
				Synchronisation.class,
				this::onSynchronisation
		).build();
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

	private void onHeartBeat(HeartBeat msg) {
		this.timeOutManager.resetCountDown(
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
		if (msg.reason() == TimeOutReason.ELECTION)
		{
			logger.log(
					LogLevel.WARN,
					"[NODE-" + this.nodeId + "] received a countdown of type " + msg.reason()
			);
		}
		int countDownIndex =
				(msg.reason() == TimeOutReason.HEARTBEAT || msg.reason() == TimeOutReason.ELECTION) ? 0 : msg.epoch()
						.i();
		this.timeOutManager.handleCountDown(
				msg.reason(),
				countDownIndex,
				this,
				this.logger
		);
	}

	private void onWriteRequest(WriteRequest msg) {
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
				"[NODE-" + this.nodeId + "] sending write request number " + numbersOfWrites + " with value " + msg.newValue()
		);
		numbersOfWrites++;
	}

	private void onWriteResponse(WriteResponse msg) {
		this.timeOutManager.resetCountDown(
				TimeOutReason.WRITE,
				msg.nRequest(),
				nodeId,
				this.logger
		);
	}

	private void onTimeOut(TimeOut msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] received a timeout " + msg.reason()
		);
		switch (msg.reason())
		{
			case WRITE:
			case HEARTBEAT:
				this.startElection();
				break;
			case ELECTION:
				this.retryElection();
				break;
			default:
				break;
		}
	}

	private void onElection(Election msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] received election message from [NODE-" + this.getSender() + "] with " +
						"params: < e :" + msg.highestEpoch() + ", i :" + msg.highestIteration() + ">, best " +
						"candidate received:" + msg.bestCandidateId()
		);
		this.tell(
				this.getSender(),
				new ElectionACK(),
				this.getSelf()
		);
		int idDest = getNextNodeForElection();
		EpochPair nodeLatest = this.history.getLatest();
		if (this.isElection)
		{
			if (msg.bestCandidateId() == this.nodeId)
			{
				this.becomeCoordinator();
				this.isElection = false;
				this.timeOutManager.endElectionState();
				/*this.epochPair = new EpochPair(
						this.epochPair.e() + 1,
						0
				);*/
				logger.log(
						LogLevel.INFO,
						"[NODE-" + this.nodeId + "] elected as supreme leader with countdown manager: " + this.timeOutManager
				);
				this.multicast(new Synchronisation(
						this.history,
						this.history.getLatest()
				));
			}
			else if (msg.highestEpoch() >= nodeLatest.e() && msg.highestIteration() >= nodeLatest.i() && msg.bestCandidateId() < this.nodeId)
			{
				this.forwardPreviousElectionMessage(
						msg,
						idDest
				);
			}
			else
			{
				logger.log(
						LogLevel.INFO,
						"[NODE-" + this.nodeId + "] is not going to forward election message from " + this.getSender()
				);
			}
		}
		else
		{
			this.timeOutManager.startElectionState();
			this.isElection = true;
			if (msg.highestEpoch() >= nodeLatest.e() && msg.highestIteration() >= nodeLatest.i() && msg.bestCandidateId() < this.nodeId)
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

	private void startElection() {
		if (! this.isElection)
		{
			logger.log(
					LogLevel.INFO,
					"[NODE-" + this.nodeId + "] started the election process, my history is: < e:" + this.history.getLatest()
							.e() + ", i:" + this.history.getLatest().i() + " >"
			);
			this.isElection = true;
			int idDest = getNextNodeForElection();
			EpochPair latest = this.history.getLatest();
			this.lastElectionData = new Utils.Quadruplet<>(
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

	private void onElectionAck(ElectionACK msg) {
		boolean res = this.timeOutManager.resetCountDown(
				TimeOutReason.ELECTION,
				0,
				this.nodeId,
				logger
		);
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] received election ACK from [NODE-" + this.getSender() + "], timer " +
						"stopped: " + res
		);
	}

	private int getNextNodeForElection() {
		int idDest = (this.nodeId + 1) % this.numberOfNodes;
		while (this.group.get(idDest) == this.coordinator && idDest != this.nodeId)
		{
			idDest = (idDest + 1) % this.numberOfNodes;
		}
		return idDest;
	}

	private void startElectionCountDown() {
		logger.log(
				LogLevel.INFO,
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

	private void retryElection() {
		int oldId = this.lastElectionData.destinationId();
		int newDestId = (oldId + 1) % numberOfNodes;
		this.lastElectionData = new Quadruplet<>(
				newDestId,
				this.lastElectionData.highestEpoch(),
				this.lastElectionData.highestIteration(),
				this.lastElectionData.bestCandidateId()
		);
		this.sendNewElectionMessage(
				new EpochPair(
						this.lastElectionData.highestEpoch(),
						this.lastElectionData.highestIteration()
				),
				newDestId
		);
	}

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

	private void forwardPreviousElectionMessage(Election msg, int idDest) {
		this.startElectionCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] forwarding election message with best candidate " + msg.bestCandidateId()
		);
		this.lastElectionData = new Utils.Quadruplet<>(
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

	private void sendNewElectionMessage(EpochPair highestData, int idDest) {
		this.startElectionCountDown();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "] switched election state, my history is :< e:" + highestData.e() + "," + highestData.i() + ">, sending new election message to [NODE-" + idDest + "]"
		);
		this.lastElectionData = new Utils.Quadruplet<>(
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

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// BEGIN COORDINATOR METHODS
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public AbstractActor.Receive coordinatorBehaviour() {
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
						"[NODE-" + this.nodeId + "][Coordinator] heartbeat"
				)
		).match(
				CountDown.class,
				//Special handler for the self sent Heartbeat, the print is just for debug
				this::coordinatorOnCountDown
		).match(
				CrashRequest.class,
				this::onCrashRequest
		).match(
				WriteRequest.class,
				this::coordinatorOnWriteRequest
		).build();

	}

	/**
	 * Initial set up for the Coordinator, should be called whenever an ActorRef becomes Coordinator.
	 *
	 * @param msg the init message
	 */
	public void coordinatorOnStartMessage(StartMessage msg) {
		this.heartBeat = new Cancellable[numberOfNodes];
		this.quorum = (numberOfNodes / 2) + 1;
		this.setGroup(msg);
		this.startHeartBeat();
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "][Coordinator] starting with " + this.group.size() + " peer(s)"
		);
	}

	/**
	 * Register a Node vote.
	 * Then, if the quorum is reached or everybody voted, fix the decision and multicast the decision.
	 *
	 * @param msg request to make a vote
	 */
	public void coordinatorOnVoteResponse(VoteResponse msg) {
		Vote v = msg.vote();
		int e = msg.epoch().e();
		int i = msg.epoch().i();
		if (this.crashType == CrashType.COORDINATOR_NO_QUORUM)
		{
			this.coordinatorCrash(
					e,
					i
			);
		}
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
			if (this.crashType == CrashType.COORDINATOR_QUORUM)
			{
				this.coordinatorCrash(
						e,
						i
				);
			}
			fixCoordinatorDecision(
					isQuorumReached ? Decision.WRITEOK : Decision.ABORT,
					e,
					i
			);
			multicast(new DecisionResponse(
					this.voters.get(e).get(i).finalDecision(),
					msg.epoch()
			));
			if (this.voters.get(e).get(i).finalDecision() == Decision.WRITEOK)
			{
				this.history.setStateToTrue(
						e,
						i
				);
			}
		}
	}

	private void coordinatorOnWriteRequest(WriteRequest msg) {
		this.tell(
				this.getSender(),
				new WriteResponse(msg.nRequest()),
				this.getSelf()
		);
		int e = this.history.isEmpty() ? 0 : this.history.size() - 1;
		int i = this.history.isEmpty() ? 0 : this.history.get(e).size();
		/*this.epochPair = new EpochPair(
				e,
				i + 1
		);*/
		this.history.insert(
				e,
				i,
				msg.newValue()
		);
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "][Coordinator] sending write response for write request number " + msg.nRequest() + " with value " + msg.newValue()
		);
		multicast(new VoteRequest(
				msg.newValue(),
				new EpochPair(e,
						i)
		));
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "][Coordinator] Sent vote request to write " + msg.newValue() + " for epoch " + "< " + e + ", " + i + " >"
		);
	}

	private void coordinatorOnCountDown(CountDown msg) {
		logger.log(
				LogLevel.INFO,
				"[NODE-" + this.nodeId + "][Coordinator] countdown of type " + msg.reason()
		);
	}

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
					"[NODE-" + this.nodeId + "][Coordinator] decided " + d + " for epoch < " + e + ", " + i + " >"
			);
		}
	}

	/**
	 * Assign to every node in the group a Heartbeat scheduled message
	 */
	private void startHeartBeat() {
		logger.log(
				LogLevel.DEBUG,
				"[NODE-" + this.nodeId + "][Coordinator] starting heartbeat protocol"
		);
		// Yes the coordinator sends a Heartbeat to itself
		for (int i = 0; i < this.numberOfNodes; ++ i)
		{
			heartBeat[i] = getContext().getSystem().scheduler().scheduleAtFixedRate(
					Duration.ZERO,
					Duration.ofMillis(HEARTBEAT_TIMEOUT / 2),
					this.group.get(i),
					new HeartBeat(),
					getContext().getSystem().dispatcher(),
					getSelf()
			);
		}
	}

	private void coordinatorCrash(int e, int i) {
		logger.log(
				LogLevel.ERROR,
				"[NODE-" + this.nodeId + "][Coordinator] CRASHED!!! on epoch " + e + " during iteration " + i
		);
		for (Cancellable heart : heartBeat)
		{
			heart.cancel();
		}
		this.crash();
	}
}
