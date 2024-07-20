package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class Utils {
    final static int HEARTBEAT_TIMEOUT = 2000; // timeout for the heartbeat, ms
    final static int ELECTION_TIMEOUT = 2000; // timeout for the heartbeat, ms
    public enum Vote {NO, YES}

    public enum Decision {ABORT, WRITEOK, PENDING}

    public enum LogLevel {
        TRACE(1),
        DEBUG(2),
        INFO(3),
        WARN(4),
        ERROR(5);

        final int level;

        LogLevel(int level) {
            this.level = level;
        }
    }

    public enum TimeOutReason {HEARTBEAT, VOTE, WRITE, ELECTION}

    public enum CrashType {
        NO_CRASH, NODE_BEFORE_WRITE_REQUEST, NODE_AFTER_WRITE_REQUEST, NODE_AFTER_VOTE_REQUEST, NODE_AFTER_VOTE_CAST
        , COORDINATOR_BEFORE_RW_REQUEST, COORDINATOR_AFTER_RW_REQUEST, COORDINATOR_NO_QUORUM, COORDINATOR_QUORUM,
    }

    // Start message that sends the list of participants to everyone
    public record StartMessage(List<ActorRef> group) implements Serializable {
        public StartMessage(List<ActorRef> group) {
            this.group = List.copyOf(group);
        }
    }

    public record VoteRequest(int newValue, EpochPair epoch) implements Serializable {

    }

    public record VoteResponse(Vote vote, EpochPair epoch) implements Serializable {

    }

    /*public record DecisionRequest() implements Serializable {

    }*/

    public record DecisionResponse(Decision decision, EpochPair epoch) implements Serializable {

    }

    public record CountDown(TimeOutReason reason, EpochPair epoch) implements Serializable {
    }

    public record TimeOut(TimeOutReason reason) implements Serializable {
    }

    public record HeartBeat() implements Serializable {
    }

    public record CrashRequest(CrashType crashType) implements Serializable {
    }

    public record MakeRequest(boolean kindOfRequest, int indexTarget) implements Serializable {
    }

    public record ReadRequest() implements Serializable {
    }

    public record WriteRequest(int newValue, int nRequest) implements Serializable {
    }

    public record WriteResponse(int nRequest) implements Serializable {
    }

    public record ReadValue(int value) implements Serializable {
    }

    public record EpochPair(int e, int i) implements Serializable {
    }

    public record Election(int highestEpoch, int highestIteration, int bestCandidateId) implements Serializable {
    }

    public record ElectionACK() implements Serializable {
    }

    public record VotePair(HashMap<ActorRef, Vote> votes, Decision finalDecision) {
    }

    public record Quadruplet<ID, E, I>(ID destinationId, E highestEpoch, I highestIteration, ID bestCandidateId) {
    }

    public record Synchronisation(PairsHistory history, EpochPair newEpochPair) implements Serializable {
    }

}

