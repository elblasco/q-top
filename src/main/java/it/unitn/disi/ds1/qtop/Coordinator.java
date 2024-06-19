package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.time.Duration;
import java.util.HashMap;

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Coordinator extends Node {
    private HashMap<ActorRef, Vote> voters = new HashMap<>();
    private int numberOfNodes;
    private int quorum;
    private Decision generalDecision = null;
    private Cancellable[] heartBeat;

    private final Logger logger = Logger.getInstance();

    public Coordinator(int nodeId, int numberOfNodes, int decisionTimeout, int voteTimeout) {
        super(
                nodeId,
                decisionTimeout,
                voteTimeout
        );
        this.numberOfNodes = numberOfNodes;
        this.quorum = (numberOfNodes / 2) + 1;
        heartBeat = new Cancellable[numberOfNodes];
    }

    static public Props props(int nodeId, int numberOfNodes, int decisionTimeout, int voteTimeout) {
        return Props.create(Coordinator.class, () -> new Coordinator(nodeId, numberOfNodes, decisionTimeout, voteTimeout));
    }

    /**
     * Fix the Coordinator decision.
     *
     * @param d decision took by the coordinator
     */
    private void fixCoordinatorDecision(Decision d) {
        if (! hasDecided())
        {
            this.generalDecision = d;
            logger.log(LogLevel.INFO,"[NODE-"+this.nodeId+"][Coordinator] decided " + d);
        }
    }

    Receive crashed() {

        for (Cancellable heart : heartBeat)
        {
            heart.cancel();
        }
        return crash();
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
                DecisionRequest.class,
                this::onDecisionRequest
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
                        Utils.CrashRequest.class,
                        super::onCrashRequest
                ).match(
                        WriteRequest.class,
                        this::onWriteRequest
                )
		   .build();
    }

    /**
     * Initial set up for the Coordinator, should be called whenever an ActorRef becomes Coordinator.
     * @param msg the init message
     */
    @Override
    public void onStartMessage(StartMessage msg) {
        super.onStartMessage(msg);
        this.startHeartBeat();

        logger.log(LogLevel.INFO,"[NODE-"+this.nodeId+"][Coordinator] starting with " + this.group.size() + " peer(s)");
        //multicast(new VoteRequest());
        //logger.log(LogLevel.INFO,
        //        "[NODE-" + this.nodeId + "][Coordinator] Sent vote request");
    }


    /**
     * Register a Node vote.
     * Then, if the quorum is reached or everybody voted, fix the decision and multicast the decision.
     * @param msg request to make a vote
     */
    public void onVoteResponse(VoteResponse msg) {
        Vote v = msg.vote();
        voters.put(getSender(), v);
        if (quorumReached() || voters.size() == numberOfNodes) {
            if (this.crashType == CrashType.COORDINATOR_QUORUM)
            {
                crash();
            }
            fixCoordinatorDecision(quorumReached() ? Decision.WRITEOK : Decision.ABORT);
            multicast(new DecisionResponse(generalDecision));
            voters = new HashMap<>();
            if (this.generalDecision == Decision.WRITEOK)
            {
                this.sharedVariable = this.possibleNewSharedVaribale;
            }
            this.possibleNewSharedVaribale = 0;
        }
        else if (this.crashType == CrashType.COORDINATOR_NO_QUORUM)
        {
            crash();
        }
    }

    /**
     * Send back to the Receiver the decision.
     * @param msg Receiver request for Coordinator decision
     */
    private void onDecisionRequest(DecisionRequest msg) {
        if (hasDecided())
        {
            this.tell(
                    getSender(),
                    new DecisionResponse(this.generalDecision),
                    getSelf()
            );
        }
    }

    private boolean hasDecided() {
        return generalDecision != null;
    }

    /**
     * Fix the Coordinator decision.
     * @param msg decision took by the Coordinator
     */
    @Override
    protected void onDecisionResponse(DecisionResponse msg) {
        super.onDecisionResponse(msg);
        fixCoordinatorDecision(msg.decision());
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
        for (int i = 0; i < numberOfNodes; ++ i) //node : group
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

    private boolean quorumReached() {
        return voters.entrySet().stream().filter(entry -> entry.getValue() == Vote.YES).toList().size() >= quorum;
    }

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     * @param msg request to make a vote
     */
    @Override
    public void onVoteRequest(VoteRequest msg) {
        super.onVoteRequest(msg);
        this.tell(
                getSelf(),
                new VoteResponse(this.nodeVote),
                getSelf()
        );
    }

    private void onReadRequest(ReadRequest msg) {
        getSender().tell(
                new ReadValue(this.sharedVariable),
                this.getSelf()
        );
    }

    private void onWriteRequest(WriteRequest msg) {
        multicast(new VoteRequest(msg.newValue()));
        logger.log(
                LogLevel.INFO,
                "[NODE-" + this.nodeId + "][Coordinator] Sent vote request to write " + msg.newValue()
        );
    }
}
