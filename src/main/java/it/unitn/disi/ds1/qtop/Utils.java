package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Class that contains all the utility classes and messages used by the actors.
 */
public class Utils {

	/**
	 * The timeout for the heartbeat.
	 */
	final static int HEARTBEAT_TIMEOUT = 1000; // timeout for the heartbeat, ms

	/**
	 * The timeout for the vote request.
	 */
	final static int ELECTION_TIMEOUT = 300; // timeout for the heartbeat, ms

	/**
	 * The timeout for the crash response.
	 */
	final static int CRASH_TIMEOUT = 300; // timeout for the heartbeat, ms

	/**
	 * Enum to represent all the possible votes that can be taken by a Node.
	 */
	public enum Vote {
		/**
		 * A Node voted no for a write request.
		 */
		NO,
		/**
		 * A Node voted yes for a write request.
		 */
		YES
	}

	/**
	 * Enum to represent all the possible decisions that can be taken by the coordinator.
	 */
	public enum Decision {
		/**
		 * The coordinator decided to abort the operation.
		 */
		ABORT,
		/**
		 * The coordinator decided to execute the operation.
		 */
		WRITEOK,
		/**
		 * The coordinator is still waiting for the votes.
		 */
		PENDING
	}

	/**
	 * Enum to represent all the log levels, selected a level all the highest levels would be printed.
	 */
	public enum LogLevel {
		/**
		 * Log level for trace messages.
		 */
		TRACE(1),
		/**
		 * Log level for debug messages.
		 */
		DEBUG(2),
		/**
		 * Log level for info messages.
		 */
		INFO(3),
		/**
		 * Log level for warning messages.
		 */
		WARN(4),
		/**
		 * Log level for error messages.
		 */
		ERROR(5);

		final int level;

		LogLevel(int level) {
			this.level = level;
		}
	}

	/**
	 * Enum to represent all the possible reasons for a timeout.
	 */
	public enum TimeOutReason {
		/**
		 * Timeout for the heartbeat.
		 */
		HEARTBEAT,
		/**
		 * Timeout for a vote request.
		 */
		VOTE,
		/**
		 * Timeout for a write request.
		 */
		WRITE,
		/**
		 * Timeout for an election message.
		 */
		ELECTION,
		/**
		 * Timeout to elect a leader.
		 */
		ELECTION_GLOBAL,
		/**
		 * Timeout for a CrashRequest message.
		 */
		CRASH_RESPONSE,
		/**
		 * Timeout to make a request via a Client.
		 */
		CLIENT_REQUEST
	}

	/**
	 * Enum to represent all the possible crashes that can be triggered.
	 */
	public enum CrashType {
		/**
		 * No crash.
		 */
		NO_CRASH,
		/**
		 * Receiver crash before propagating a write request.
		 */
		NODE_BEFORE_WRITE_REQUEST,
		/**
		 * Receiver crash after propagating a write request.
		 */
		NODE_AFTER_WRITE_REQUEST,
		/**
		 * Receiver crash after receiving a vote request.
		 */
		NODE_AFTER_VOTE_REQUEST,
		/**
		 * Receiver crash after casting a vote.
		 */
		NODE_AFTER_VOTE_CAST,
		/**
		 * Probabilistic crash during a vote request multicast.
		 */
		COORDINATOR_ON_VOTE_REQUEST,
		/**
		 * Probabilistic crash during a decision multicast.
		 */
		COORDINATOR_ON_DECISION_RESPONSE,
	}

	/**
	 * Start message that sends the list of participants to everyone.
	 */
	public record StartMessage(List<ActorRef> group) implements Serializable {
		public StartMessage(List<ActorRef> group) {
			this.group = List.copyOf(group);
		}
	}

	/**
	 * Message that asks for a vote.
	 *
	 * @param newValue new value proposed
	 * @param epoch    epoch associated to the request
	 */
	public record VoteRequest(int newValue, EpochPair epoch) implements Serializable {
	}

	/**
	 * Message to send the vote of a node to the coordinator.
	 *
	 * @param vote  the vote of the node
	 * @param epoch the epoch associated to the vote
	 */
	public record VoteResponse(Vote vote, EpochPair epoch) implements Serializable {
	}

	/**
	 * Message to propagate the coordinator final decision regarding a certain epoch.
	 *
	 * @param decision the decision of the coordinator
	 * @param epoch    epoch associated to the request
	 */
	public record DecisionResponse(Decision decision, EpochPair epoch) implements Serializable {
	}

	/**
	 * Message self-lopped by a node to trigger a timeout.
	 *
	 * @param reason the reason for the timeout
	 * @param epoch  (optional) epoch associated to it
	 */
	public record CountDown(TimeOutReason reason, EpochPair epoch) implements Serializable {
	}

	/**
	 * Message to notify a timeout.
	 *
	 * @param reason the reason for the timeout
	 * @param epoch  (optional) epoch associated to it
	 */
	public record TimeOut(TimeOutReason reason, EpochPair epoch) implements Serializable {
	}

	/**
	 * Special message to multicast a heartbeat from a coordinator to all the nodes.
	 */
	public record HeartBeat() implements Serializable {
	}

	/**
	 * Message to ask for a crash.
	 */
	public record CrashRequest(CrashType crashType) implements Serializable {
	}

	/**
	 * Message used by a Client to generate a random request to a random Node.
	 */
	public record MakeRequest() implements Serializable {
	}

	/**
	 * Message used by a Client to ask for a read operation.
	 *
	 * @param nRequest the number of the request
	 */
	public record ReadRequest(int nRequest) implements Serializable {
	}

	/**
	 * Message used by a Client to ask for a read operation.
	 *
	 * @param newValue the new value proposed by the Client
	 * @param nRequest the number of the request
	 */
	public record WriteRequest(int newValue, int nRequest) implements Serializable {
	}

	/**
	 * Message used by a Node to notify the Client that it received the ReadRequest, and reply to it.
	 *
	 * @param value    the value read by the Node
	 * @param nRequest the number of the request
	 */
	public record ReadValue(int value, int nRequest) implements Serializable {
	}

	/**
	 * Message used by a Node to notify the Client that it received the WriteRequest, and reply to it.
	 *
	 * @param value    the value written by the Node
	 * @param nRequest the number of the request
	 */
	public record WriteValue(int value, int nRequest) implements Serializable {
	}

	/**
	 * Struct to represent and send an epoch and an iteration.
	 *
	 * @param e epoch
	 * @param i iteration
	 */
	public record EpochPair(int e, int i) implements Serializable {
		/**
		 * Copy constructor
		 *
		 * @param ep the EpochPair to copy
		 */
		public EpochPair(EpochPair ep) {
			this(
					ep.e(),
					ep.i()
			);
		}
	}

	/**
	 * Message to send election data
	 *
	 * @param highestEpoch     highest epoch known
	 * @param highestIteration highest iteration known
	 * @param bestCandidateId  node that have that EpochPair
	 */
	public record Election(int highestEpoch, int highestIteration, int bestCandidateId) implements Serializable {
		public boolean isGreaterThanLocalData(int nodeId, @NotNull EpochPair ep) {
			return ((this.highestEpoch() > ep.e()) || (this.highestEpoch() == ep.e() && this.highestIteration() > ep.i()) || (this.highestEpoch() == ep.e() && this.highestIteration() == ep.i() && this.bestCandidateId() < nodeId));
		}
	}

	/**
	 * Message to acknowledge an Election
	 */
	public record ElectionACK() implements Serializable {
	}

	/**
	 * Message to synchronise all the node after an election.
	 *
	 * @param history      the new history of transactions
	 * @param newEpochPair the new epoch pair
	 */
	public record Synchronisation(PairsHistory history, EpochPair newEpochPair) implements Serializable {
	}

	/**
	 * Message to acknowledge a CrashRequest.
	 */
	public record CrashACK() implements Serializable {
	}

	/**
	 * Struct to represent the map of voters for every write, and the final decision taken for that write.
	 *
	 * @param votes         map of voters
	 * @param finalDecision coordinator final decision
	 */
	public record VotePair(HashMap<ActorRef, Vote> votes, Decision finalDecision) {
	}

	/**
	 * Struct cache the best candidate data during the election.
	 *
	 * @param destinationId    the next node to send the election message
	 * @param highestEpoch     highest epoch known
	 * @param highestIteration highest iteration known
	 * @param bestCandidateId  node that have that EpochPair
	 */
	public record Quadruplet(int destinationId, int highestEpoch, int highestIteration, int bestCandidateId) {
	}
}

