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
    private final int decisionTimeout;
    public Utils.CrashType crashType = CrashType.NO_CRASH;
    public boolean crashed = false;
    private final int voteTimeout;
    public static Random rand = new Random();
    private PairsHistory history;

    private final Logger logger = Logger.getInstance();

    public Node(int nodeId, int decisionTimeout, int voteTimeout) {
        super();
        this.history = new PairsHistory();
        this.viewId = 0;
        this.nodeId = nodeId;
        this.decisionTimeout = decisionTimeout;
        this.voteTimeout = voteTimeout;
    }

    /**
     * Make a vote, fix it and then send it back to the coordinator.
     *
     * @param msg request to make a vote
     */
    protected void onVoteRequest(VoteRequest msg) {
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
                        .e() + ", " + msg.epoch().i() + " > and variable " + msg.newValue()
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
            Thread.sleep(rand.nextInt(100));
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    protected void onDecisionResponse(DecisionResponse msg) {
        logger.log(
                LogLevel.INFO,
                "[NODE-" + this.nodeId + "] decided " + msg.decision() + " for epoch < " + msg.epoch()
                        .e() + ", " + msg.epoch().i() + " >"
        );
        if (msg.decision() == Decision.WRITEOK)
        {
            int e = msg.epoch().e();
            int i = msg.epoch().i();
            this.getHistory().setStateToTrue(
                    e,
                    i
            );
            logger.log(
                    LogLevel.INFO,
                    "[NODE-" + this.nodeId + "] committed shared variable " + this.getHistory().get(e).get(i).number()
            );
        }
    }

    public PairsHistory getHistory() {
        return history;
    }

    protected void onCrashRequest(CrashRequest msg) {
        this.crashType = msg.crashType();
		logger.log(LogLevel.INFO, "[NODE-" + this.nodeId+ "] crashed because " + this.crashType);
    }

    protected void onReadRequest(ReadRequest msg) {
        getSender().tell(
                new ReadValue(this.getHistory().readValidVariable()),
                this.getSelf()
        );
    }

    Receive crash() {
        this.crashed = true;
        return receiveBuilder().matchAny(msg -> {
        }).build();
    }
}
