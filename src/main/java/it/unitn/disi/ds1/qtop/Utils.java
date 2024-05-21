package it.unitn.disi.ds1.qtop;

import java.io.Serializable;
import java.util.List;

import akka.actor.ActorRef;

public class Utils {
    final static int N_NODES = 5;
    final static int VOTE_TIMEOUT = 1000;      // timeout for the votes, ms
    final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms
    final static int QUORUM = (N_NODES / 2) + 1;

    public enum Vote {NO, YES}
    public enum Decision {ABORT, WRITEOK}
    // Start message that sends the list of participants to everyone
    public record StartMessage(List<ActorRef> group) implements Serializable {
        public StartMessage(List<ActorRef> group) {
            this.group = List.copyOf(group);
        }
    }


    public static class VoteRequest implements Serializable {}

    public static class VoteResponse implements Serializable {
        public final Vote vote;
        public VoteResponse(Vote v) { vote = v; }
    }

    public static class DecisionRequest implements Serializable {}

    public static class DecisionResponse implements Serializable {
        public final Decision decision;
        public DecisionResponse(Decision d) { decision = d; }
    }

    public static class Timeout implements Serializable {}

    public static class Recovery implements Serializable {}
}
