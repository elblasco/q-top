package it.unitn.disi.ds1.qtop;

import static it.unitn.disi.ds1.qtop.Utils.*;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;

public class Coordinator extends Node {

    private HashMap<ActorRef, Vote> voters = new HashMap<>();
    private Decision generalDecision = null;

    public Coordinator(int nodeId) {
        super(nodeId);
    }

    static public Props props(int nodeId) {
        return Props.create(Coordinator.class, () -> new Coordinator(nodeId));
    }

    /**
     * Initial set up for the Coordinator, should be called whenever an ActorRef becomes Coordinator.
     * @param msg the init message
     */
    @Override
    public void onStartMessage(StartMessage msg) {
        super.onStartMessage(msg);
        System.out.println(this.nodeId + " received a start message");
        System.out.println(this.nodeId + " Sending vote request");
        multicast(new VoteRequest());
    }

    /**
     * Fix the Coordinator decision.
     * @param d decision took by the coordinator
     */
    private void fixCoordinatorDecision(Decision d) {
        if (!hasDecided()) {
            this.generalDecision = d;
            System.out.println(this.nodeId + " decided " + d);
        }
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

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::onStartMessage)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .build();
    }
}
