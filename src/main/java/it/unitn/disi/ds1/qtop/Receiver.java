package it.unitn.disi.ds1.qtop;

import static it.unitn.disi.ds1.qtop.Utils.*;

import akka.actor.ActorRef;
import akka.actor.Props;

public class Receiver extends Node{
    private ActorRef coordinator;

    public Receiver(int nodeId, ActorRef coordinator) {
        super(nodeId);
        this.coordinator = coordinator;
    }

    static public Props props(int nodeId, ActorRef coordinator) {
        return Props.create(Receiver.class,() -> new Receiver(nodeId, coordinator));
    }

    /**
     * Initial set up for a Receiver, should be called whenever an ActorRef becomes a Receiver.
     * @param msg the init message
     */
    public void onStartMessage(StartMessage msg) {
        setGroup(msg);
        System.out.println(this.nodeId + " received a start message");
    }

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     * @param msg request to make a vote
     */
    public void onVoteRequest(VoteRequest msg) {
        Vote vote = vote();
        fixVote(vote);
        System.out.println(this.nodeId + " sending vote " + vote);
        this.coordinator.tell(new VoteResponse(vote), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::onStartMessage)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .build();
    }
}
