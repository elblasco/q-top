package it.unitn.disi.ds1.qtop;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import static it.unitn.disi.ds1.qtop.Utils.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Node extends AbstractActor {

    public record IdentificationPair(int e, int i){
        public IdentificationPair(int e, int i){this.e=e;this.i=i;}
    }

    // participants (initial group, current and proposed views)
    private List<ActorRef> group;
    private HashMap<ActorRef, Vote> voters = new HashMap<>();
    private ActorRef coordinator = null;
    private int viewId;
    private int nodeId;
    private boolean isCoordinator;
    private IdentificationPair pair;
    private Decision decision = null;


    /*-- Actor constructors --------------------------------------------------- */
    public Node(int nodeId) {
        super();
        viewId = 0;
        this.nodeId = nodeId;
        isCoordinator = false;
    }

    static public Props props(int nodeId) {
        return Props.create(Node.class,() -> new Node(nodeId));
    }
    /*------------------------------------------------------------------------ */

    private void setGroup(StartMessage sm) {
        this.group = new ArrayList<>();
        for (ActorRef node: sm.group()) {
            if (!node.equals(getSelf())) {

                // copying all participant refs except for self
                this.group.add(node);
            }
        }
        System.out.println(this.nodeId + " starting with " + sm.group().size() + " peer(s)");
    }

    public void onStartMessage(StartMessage msg) {                   /* Start */
        setGroup(msg);
        System.out.println(this.nodeId + " Sending vote request");
        multicast(new VoteRequest());
        //multicastAndCrash(new VoteRequest(), 3000);
        setTimeout(VOTE_TIMEOUT);
        //crash(5000);
    }

    // schedule a Timeout message in specified time
    private void setTimeout(int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    private void multicast(Serializable m) {
        for (ActorRef node: group)
            node.tell(m, getSelf());
    }

    private boolean hasDecided() { return decision != null; } // has the node decided?

    private Vote vote(){
        List<Vote> VALUES = List.of(Vote.values());
        int SIZE = VALUES.size();
        Random RANDOM = new Random();
        return VALUES.get(RANDOM.nextInt(SIZE));
    }

    // fix the final decision of the current node
    void fixDecision(Decision d) {
        if (!hasDecided()) {
            this.decision = d;
            System.out.println(this.nodeId + " decided " + d);
        }
    }

    public void onVoteRequest(VoteRequest msg) {
        this.coordinator = getSender();
        Vote decision = vote();
        //if (id==2) {crash(5000); return;}    // simulate a crash
        //if (id==2) delay(4000);              // simulate a delay
        if (decision == Vote.NO) {
            fixDecision(Decision.ABORT);
        }
        System.out.println(this.nodeId + " sending vote " + decision);
        this.coordinator.tell(new VoteResponse(decision), getSelf());
        setTimeout(DECISION_TIMEOUT);
    }

    private boolean quorumReached() { // returns true if all voted YES
        return voters.entrySet().stream().filter(entry -> entry.getValue() == Vote.YES).toList().size() >= QUORUM;
    }


    public void onVoteResponse(VoteResponse msg) {                    /* Vote */
        if (hasDecided()) {
            return;
        }
        Vote v = (msg).vote;
        if (v == Vote.YES || v == Vote.NO) {
            voters.put(getSender(), v);
            if (quorumReached()) {
                voters = new HashMap<>();
                fixDecision(Decision.COMMIT);
                //if (id==-1) {crash(3000); return;}
                multicast(new DecisionResponse(decision));
                //multicastAndCrash(new DecisionResponse(decision), 3000);
            }
        }
        if (voters.size() == N_NODES && !quorumReached()) {
            voters = new HashMap<>();
            fixDecision(Decision.ABORT);
            //if (id==-1) {crash(3000); return;}
            multicast(new DecisionResponse(decision));
        }
    }

    public void onDecisionRequest(DecisionRequest msg) {  /* Decision Request */
        if (hasDecided())
            getSender().tell(new DecisionResponse(decision), getSelf());

        // just ignoring if we don't know the decision
    }

    public void onDecisionResponse(DecisionResponse msg) { /* Decision Response */

        // store the decision
        fixDecision(msg.decision);
    }

    public void onTimeout(Timeout msg) {
        if (!hasDecided()) {
            if(isCoordinator){
                System.out.println("Timeout");
                //multicast(Decision.ABORT);
                // TODO 1: coordinator timeout action
            }
            else{
                System.out.println("Timeout. Asking around.");
                //multicast(new DecisionRequest());
                // TODO 3: participant termination protocol
            }
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::onStartMessage)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .match(Timeout.class, this::onTimeout)
                .build();
    }
}
