package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class Utils {
    final static int HEARTBEAT_TIMEOUT = 1000; // timeout for the heartbeat, ms
    public enum Vote {NO, YES}
    public enum Decision {ABORT, WRITEOK}

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

    public enum TimeOutAndTickReason {HEARTBEAT}

    // Start message that sends the list of participants to everyone
    public record StartMessage(List<ActorRef> group) implements Serializable {
        public StartMessage(List<ActorRef> group) {
            this.group = List.copyOf(group);
        }
    }

    public record IdentificationPair(int e, int i) {

    }

    public record VoteRequest() implements Serializable {

    }

    public record VoteResponse(Vote vote) implements Serializable {

    }

    public record DecisionRequest() implements Serializable {

    }

    public record DecisionResponse(Decision decision) implements Serializable {

    }

    public record CountDown(TimeOutAndTickReason reason) implements Serializable {
    }

    public record HeartBeat() implements Serializable {
    }

    public static class ForwardUpdate implements Serializable {

    }
}
