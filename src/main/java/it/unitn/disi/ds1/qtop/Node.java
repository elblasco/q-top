package it.unitn.disi.ds1.qtop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static it.unitn.disi.ds1.qtop.Utils.*;

abstract public class Node extends AbstractActor {

    protected static final int COUNTDOWN_REFRESH = 10;
    protected final int writeTimeout;
    protected List<ActorRef> group;
    protected int viewId;
    protected final int nodeId;
    private final int decisionTimeout;
    public Utils.CrashType crashType = CrashType.NO_CRASH;
    public boolean crashed = false;
    private final int voteTimeout;
    private int numberOfNodes;
    public static Random rand = new Random();
    private PairsHistory history;
    protected TimeOutManager timeouts;

    private final Logger logger = Logger.getInstance();

    public Node(int nodeId, int decisionTimeout, int voteTimeout, int writeTimeout) {
        super();
        this.history = new PairsHistory();
        this.viewId = 0;
        this.nodeId = nodeId;
        this.decisionTimeout = decisionTimeout;
        this.voteTimeout = voteTimeout;
        this.writeTimeout = writeTimeout;
        this.timeouts = new TimeOutManager(
                decisionTimeout,
                voteTimeout,
                HEARTBEAT_TIMEOUT,
                writeTimeout,
                Node.COUNTDOWN_REFRESH
        );
    }


    /**
     * Initial set up for a Node.
     *
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
        this.startDecisionCountDown(
                msg.epoch().i(),
                msg.epoch().e()
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

    protected void startDecisionCountDown(int i, int e) {
        this.timeouts.startCountDown(
                TimeOutReason.DECISION,
                this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
                        Duration.ZERO,
                        Duration.ofMillis(this.decisionTimeout / COUNTDOWN_REFRESH),
                        getSelf(),
                        new CountDown(
                                TimeOutReason.DECISION,
                                new EpochPair(
                                        e,
                                        i
                                )
                        ),
                        getContext().getSystem().dispatcher(),
                        getSelf()
                ),
                i
        );
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

    protected void crash() {
        this.crashed = true;
        this.getContext().become(receiveBuilder().matchAny(msg -> {
        }).build());
    }

    public void tell(ActorRef dest, final Object msg, final ActorRef sender) {
        dest.tell(
                msg,
                sender
        );
        try
        {
            Thread.sleep(rand.nextInt(200));
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    protected void onDecisionResponse(DecisionResponse msg) {
        int e = msg.epoch().e();
        int i = msg.epoch().i();
        this.timeouts.deleteCountDown(
                TimeOutReason.DECISION,
                i,
                nodeId,
                logger
        );
        logger.log(
                LogLevel.INFO,
                "[NODE-" + this.nodeId + "] decided " + msg.decision() + " for epoch < " + msg.epoch()
                        .e() + ", " + msg.epoch().i() + " >"
        );
        if (msg.decision() == Decision.WRITEOK)
        {
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

    protected void startHeartBeatCountDown() {
        this.timeouts.startCountDown(
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

    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }

    public void setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }
}
