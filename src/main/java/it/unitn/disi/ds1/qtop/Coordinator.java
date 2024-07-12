package it.unitn.disi.ds1.qtop;

import akka.actor.Cancellable;
import akka.actor.Props;

import java.time.Duration;

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Coordinator extends Node {
    private VotersMap voters = new VotersMap();
    private int quorum;
    private Cancellable[] heartBeat;
    private EpochPair epochPair;

    private final Logger logger = Logger.getInstance();

    public Coordinator(int nodeId, int numberOfNodes, int decisionTimeout, int voteTimeout, int writeTimeout) {
        super(
                nodeId,
                decisionTimeout,
                voteTimeout,
                writeTimeout
        );
        super.setNumberOfNodes(numberOfNodes);
        this.quorum = (numberOfNodes / 2) + 1;
        this.heartBeat = new Cancellable[numberOfNodes];
    }

    static public Props props(int nodeId, int numberOfNodes, int decisionTimeout, int voteTimeout, int writeTimeout) {
        return Props.create(
                Coordinator.class,
                () -> new Coordinator(
                        nodeId,
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
                VoteResponse.class,
                this::onVoteResponse
        ).match(
                VoteRequest.class,
                this::onVoteRequest
        ).match(
                ReadRequest.class,
                super::onReadRequest
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
                this::onCountDown
        ).match(
                CrashRequest.class,
                super::onCrashRequest
        ).match(
                WriteRequest.class,
                this::onWriteRequest
        ).build();
    }

    /**
     * Initial set up for the Coordinator, should be called whenever an ActorRef becomes Coordinator.
     * @param msg the init message
     */
    @Override
    public void onStartMessage(StartMessage msg) {
        super.onStartMessage(msg);
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
    public void onVoteResponse(VoteResponse msg) {
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
        if ((isQuorumReached || voters.get(e).get(i).votes().size() == super.getNumberOfNodes()) && this.voters.get(e)
                .get(i)
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
                this.getHistory().setStateToTrue(
                        e,
                        i
                );
            }
        }
        else if (this.crashType == CrashType.COORDINATOR_NO_QUORUM)
        {
            this.coordinatorCrash(
                    e,
                    i
            );
        }
    }
    
    protected void onWriteRequest(WriteRequest msg) {
        super.tell(
                this.getSender(),
                new WriteResponse(msg.nRequest()),
                this.getSelf()
        );
        int e = this.getHistory().isEmpty() ? 0 : this.getHistory().size() - 1;
        int i = this.getHistory().isEmpty() ? 0 : this.getHistory().get(e).size();
        this.epochPair = new EpochPair(
                e,
                i
        );
        this.getHistory().insert(
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
                epochPair
        ));
        logger.log(
                LogLevel.INFO,
                "[NODE-" + this.nodeId + "][Coordinator] Sent vote request to write " + msg.newValue() + " for epoch "
                        + "< " + e + ", " + i + " >"
        );
    }

    private void onCountDown(CountDown msg) {
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
        for (int i = 0; i < super.getNumberOfNodes(); ++ i)
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
        super.crash();
    }
}
