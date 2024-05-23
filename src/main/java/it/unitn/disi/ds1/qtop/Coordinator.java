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

    public void onStartMessage(StartMessage msg) {                   /* Start */
        this.setGroup(msg);
        System.out.println(this.nodeId + " received a start message");
        System.out.println(this.nodeId + " Sending vote request");
        multicast(new VoteRequest());
    }

    private void fixCoordinatorDecision(Decision d) {
        if (!hasDecided()) {
            this.generalDecision = d;
            System.out.println(this.nodeId + " decided " + d);
        }
    }

    public void onVoteResponse(VoteResponse msg) {                    /* Vote */
        Vote v = msg.vote();
        voters.put(getSender(), v);
        if (quorumReached() || voters.size() == N_NODES) {
            fixCoordinatorDecision(quorumReached() ? Decision.WRITEOK : Decision.ABORT);
            multicast(new DecisionResponse(generalDecision));
            voters = new HashMap<>();
        }
    }

    public void onVoteRequest(VoteRequest msg) {
        Vote vote = vote();
        fixVote(vote);
        System.out.println(this.nodeId + " sending vote " + vote);
        getSelf().tell(new VoteResponse(vote), getSelf());
    }

    private boolean hasDecided() {
        return generalDecision != null;
    }

    @Override
    protected void onDecisionResponse(DecisionResponse msg) { //* Decision Response *//*
        super.onDecisionResponse(msg);
        fixCoordinatorDecision(msg.decision());
    }

    private void onDecisionRequest(DecisionRequest msg) {  /* Decision Request */
        if (hasDecided())
            getSender().tell(new DecisionResponse(this.generalDecision), getSelf());
    }

    private boolean quorumReached() { // returns true if all voted YES
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
