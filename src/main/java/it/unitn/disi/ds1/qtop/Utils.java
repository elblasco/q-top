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

    public record VoteRequest() implements Serializable {}

    public record VoteResponse(Vote vote) implements Serializable {}

    public record DecisionRequest() implements Serializable {}

    public record DecisionResponse(Decision decision) implements Serializable {}

    public static class Timeout implements Serializable {}

    public static class ForwardUpdate implements Serializable {}
}
