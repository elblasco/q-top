package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.time.Duration;
import java.util.HashMap;

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Coordinator extends Node {

    private HashMap<ActorRef, Vote> voters = new HashMap<>();
    private Decision generalDecision = null;
    private Cancellable[] heartBeat = new Cancellable[N_NODES];

    public Coordinator(int nodeId) {
        super(nodeId);
    }

    static public Props props(int nodeId) {
        return Props.create(Coordinator.class, () -> new Coordinator(nodeId));
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
            System.out.println(this.nodeId + " decided " + d);
            // TODO temporary crash
            getContext().become(crashed());
        }
    }

    // TODO temporary state of crash
    private Receive crashed() {
        for (Cancellable heart : heartBeat)
        {
            heart.cancel();
        }
        return receiveBuilder().matchAny(msg -> {
        }).build();
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
                DecisionResponse.class,
                this::onDecisionResponse
        ).match(
                HeartBeat.class,
                //Special handler for the self sent Heartbeat, the print is just for debug
                heartBeat -> System.out.println("Coordinator sent an heartbeat message")
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
        System.out.println(this.nodeId + " received a start message");
        System.out.println(this.nodeId + " Sending vote request");
        multicast(new VoteRequest());
    }


    /**
     * Register a Node vote.
     * Then, if the quorum is reached or everybody voted, fix the decision and multicast the decision.
     * @param msg request to make a vote
     */
    public void onVoteResponse(VoteResponse msg) {
        Vote v = msg.vote();
        voters.put(getSender(), v);
        if (quorumReached() || voters.size() == N_NODES) {
            fixCoordinatorDecision(quorumReached() ? Decision.WRITEOK : Decision.ABORT);
            multicast(new DecisionResponse(generalDecision));
            voters = new HashMap<>();
        }
    }

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     * @param msg request to make a vote
     */
    @Override
    public void onVoteRequest(VoteRequest msg) {
        super.onVoteRequest(msg);
        getSelf().tell(new VoteResponse(this.nodeVote), getSelf());
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
     * Send back to the Receiver the decision.
     * @param msg Receiver request for Coordinator decision
     */
    private void onDecisionRequest(DecisionRequest msg) {
        if (hasDecided())
            getSender().tell(new DecisionResponse(this.generalDecision), getSelf());
    }

    private boolean quorumReached() {
        return voters.entrySet().stream().filter(entry -> entry.getValue() == Vote.YES).toList().size() >= QUORUM;
    }

    /**
     * Assign to every node in the group a Heartbeat scheduled message
     */
    private void startHeartBeat() {
        System.out.println("Coordinator started heartbeat protocol");
        // Yes the coordinator sends a Heartbeat to itself
        for (int i = 0; i < N_NODES; ++ i) //node : group
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
}
