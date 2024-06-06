package it.unitn.disi.ds1.qtop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static it.unitn.disi.ds1.qtop.Utils.*;

abstract public class Node extends AbstractActor {

    protected List<ActorRef> group;
    protected int viewId;
    protected final int nodeId;
    protected IdentificationPair pair;
    protected Vote nodeVote = null;
    public int sharedVariable;
    public Utils.CrashType crashType = CrashType.NO_CRASH;
    public boolean crashed = false;
    public static Random rand = new Random();

    private final Logger logger = Logger.getInstance();

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
    }

    protected void multicast(Serializable m) {
        for (ActorRef node : group)
        {
            this.tell(
                    node,
                    m,
                    getSelf()
            );
        }
    }

    public void tell(ActorRef dest, final Object msg, final ActorRef sender) {
        dest.tell(
                msg,
                sender
        );
        try
        {
            Thread.sleep(rand.nextInt(1000));
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private boolean hasVoted() { return nodeVote != null; } // has the node decided?

    protected Vote vote(){
        List<Vote> VALUES = List.of(Vote.values());
        int SIZE = VALUES.size();
        return VALUES.get(rand.nextInt(SIZE));
    }

    // fix the final decision of the current node
    protected void fixVote(Vote v) {
        if (!hasVoted()) {
            this.nodeVote = v;
        }
    }

    protected void onDecisionResponse(DecisionResponse msg) {
        logger.log(LogLevel.INFO,"[NODE-"+this.nodeId+"] decided " + msg.decision());
    }

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     * @param msg request to make a vote
     */
    protected void onVoteRequest(VoteRequest msg) {
        Vote vote = vote();
        fixVote(vote);
        logger.log(LogLevel.INFO,"[NODE-"+this.nodeId+"] sending vote " + vote);
    }

    protected void onCrashRequest(CrashRequest msg) {
        this.crashType = msg.crashType();
    }

    Receive crash() {
        this.crashed = true;
        return receiveBuilder().matchAny(msg -> {
        }).build();
    }
}
