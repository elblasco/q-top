package it.unitn.disi.ds1.qtop;

import akka.actor.*;
import static it.unitn.disi.ds1.qtop.Utils.*;

import java.io.Serializable;
import java.util.*;

abstract public class Node extends AbstractActor {

    protected List<ActorRef> group;
    protected int viewId;
    protected final int nodeId;
    protected IdentificationPair pair;
    protected Vote nodeVote = null;
    public int sharedVariable;

    public Node(int nodeId) {
        super();
        viewId = 0;
        this.nodeId = nodeId;
    }

    /**
     * Initial set up for a Node.
     * @param msg the init message
     */
    protected void onStartMessage(StartMessage msg) {
        this.setGroup(msg);
    }

    protected void setGroup(StartMessage sm) {
        this.group = new ArrayList<>();
        this.group.addAll(sm.group());
        System.out.println(this.nodeId + " starting with " + sm.group().size() + " peer(s)");
    }

    protected void multicast(Serializable m) {
        for (ActorRef node: group)
            node.tell(m, getSelf());
    }

    private boolean hasVoted() { return nodeVote != null; } // has the node decided?

    protected Vote vote(){
        List<Vote> VALUES = List.of(Vote.values());
        int SIZE = VALUES.size();
        Random RANDOM = new Random();
        return VALUES.get(RANDOM.nextInt(SIZE));
    }

    // fix the final decision of the current node
    protected void fixVote(Vote v) {
        if (!hasVoted()) {
            this.nodeVote = v;
        }
    }

    protected void onDecisionResponse(DecisionResponse msg) {
        System.out.println(this.nodeId + " decided " + msg.decision());
    }

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     * @param msg request to make a vote
     */
    protected void onVoteRequest(VoteRequest msg) {
        Vote vote = vote();
        fixVote(vote);
        System.out.println(this.nodeId + " sending vote " + vote);
    }
}
