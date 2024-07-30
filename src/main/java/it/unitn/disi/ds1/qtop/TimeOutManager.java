package it.unitn.disi.ds1.qtop;

import akka.actor.Cancellable;
import akka.japi.Pair;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;

/**
 * TimeOutManager class to manage the timeouts of a Node
 */
public class TimeOutManager extends EnumMap<Utils.TimeOutReason, ArrayList<Pair<Cancellable, Integer>>> {
	// Phony map to associate a reason with its specific refresh ratio
	private final EnumMap<Utils.TimeOutReason, Integer> customTimeouts;
	private final int refresh;
	private final static Logger logger = Logger.getInstance();

	public TimeOutManager(int voteTimeout, int heartbeatTimeout, int writeTimeout, int crashResponseTimeout,
			int clientRequestTimeout, int refresh) {
		super(Utils.TimeOutReason.class);
		this.customTimeouts = new EnumMap<>(Utils.TimeOutReason.class);
		customTimeouts.put(
				Utils.TimeOutReason.VOTE,
				voteTimeout
		);
		customTimeouts.put(
				Utils.TimeOutReason.HEARTBEAT,
				heartbeatTimeout
		);
		customTimeouts.put(
				Utils.TimeOutReason.WRITE,
				writeTimeout
		);
		customTimeouts.put(
				Utils.TimeOutReason.ELECTION,
				Utils.ELECTION_TIMEOUT
		);
		customTimeouts.put(
				Utils.TimeOutReason.CRASH_RESPONSE,
				crashResponseTimeout
		);
		customTimeouts.put(
				Utils.TimeOutReason.CLIENT_REQUEST,
				clientRequestTimeout
		);
		this.refresh = refresh;
		for (Utils.TimeOutReason reason : Utils.TimeOutReason.values())
		{
			this.put(
					reason,
					new ArrayList<>()
			);
		}
	}

	/**
	 * Start a count-down for a specific reason.
	 *
	 * @param reason the reason for the count-down
	 * @param action the action to be performed when the count-down reaches 0
	 * @param i      the index of the count-down
	 */
	public void startCountDown(Utils.TimeOutReason reason, Cancellable action, int i) {
		if (this.get(reason).isEmpty() || this.get(reason).size() <= i)
		{
			int initialSize = this.get(reason).size();
			for (int x = 0; x <= (i - initialSize); x++)
			{
				// Padding of empty pairs
				this.get(reason).add(new Pair<>(
						null,
						this.customTimeouts.get(reason)
				));
			}
		}
		if (this.get(reason).get(i).first() != null)
		{
			this.get(reason).get(i).first().cancel();
		}
		this.get(reason).set(
				i,
				new Pair<>(
				action,
				this.customTimeouts.get(reason)
				)
		);
	}

	/**
	 * Decrease the time left for a specific count-down.
	 *
	 * @param reason the reason for the count-down
	 * @param i      the index of the count-down
	 * @param node   the node that is handling the count-down
	 */
	public void handleCountDown(Utils.TimeOutReason reason, int i, Node node) {
		if (this.get(reason).get(i).second() <= 0)
		{
			this.get(reason).get(i).first().cancel();
				node.tell(
						node.getSelf(),
						new Utils.TimeOut(
								reason,
								new Utils.EpochPair(
										0,
										i
								)
						),
						node.getSelf()
				);
			}
			else
			{
				this.get(reason).set(
						i,
						new Pair<>(
								this.get(reason).get(i).first(),
								this.get(reason).get(i).second() - (this.customTimeouts.get(reason) / this.refresh)
						)
				);
			}
	}

	/**
	 * Client decrease the time left for a specific count-down.
	 * @param reason the reason for the count-down
	 * @param i      the index of the count-down
	 * @param node   the node that is handling the count-down
	 */
	public void clientHandleCountDown(Utils.TimeOutReason reason, int i, Client node) {
		if (this.get(reason).get(i).second() <= 0)
		{
			this.get(reason).get(i).first().cancel();
			node.tell(
					node.getSelf(),
					new Utils.TimeOut(reason,new Utils.EpochPair(0,i)),
					node.getSelf()
			);
		}
		else
		{
			this.get(reason).set(
					i,
					new Pair<>(
							this.get(reason).get(i).first(),
							this.get(reason).get(i).second() - (this.customTimeouts.get(reason) / this.refresh)
					)
			);
		}

	}

	/**
	 * Reset a specific count down.
	 *
	 * @param reason the reason for the count-down
	 * @param i    the index of the count-down
	 *
	 * @return true if the count-down was successfully reset, false otherwise
	 */
	public boolean resetCountDown(Utils.TimeOutReason reason, int i) {
		boolean ret;
		if (reason == Utils.TimeOutReason.HEARTBEAT)
		{
			// in the HEARTBEAT case the countdown is reset to its max
				this.get(reason).set(
						i,
						new Pair<>(
								this.get(reason).get(i).first(),
								this.customTimeouts.get(reason)
						)
				);
			ret = true;
		}
		else
		{
			// in all other cases the timeout countdown is cancelled
			this.get(reason).get(i).first().cancel();
			ret = this.get(reason).get(i).first().isCancelled();
		}
		return ret;
	}

	/**
	 * Reset all the count-downs to enter the election state.
	 */
	public void startElectionState() {
		for (Map.Entry<Utils.TimeOutReason, ArrayList<Pair<Cancellable, Integer>>> entry : this.entrySet())
		{
			if (entry.getKey() != Utils.TimeOutReason.ELECTION)
			{
				for (Pair<Cancellable, Integer> element : entry.getValue())
				{
					if (element.first() != null)
					{
						element.first().cancel();
					}
				}
			}
		}
	}

	/**
	 * Reset the data structure to start a new post election phase.
	 */
	public void endElectionState() {
		this.get(Utils.TimeOutReason.ELECTION).getFirst().first().cancel();
		logger.log(
				Utils.LogLevel.INFO,
				"[NODE] canceled its election timeout " + this.get(Utils.TimeOutReason.ELECTION).getFirst().first()
						.isCancelled()
		);
		for (Utils.TimeOutReason reason : Utils.TimeOutReason.values())
		{
			if (reason != Utils.TimeOutReason.ELECTION)
			{
				this.get(reason).clear();
				this.put(
						reason,
						new ArrayList<>()
				);
			}
		}


	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<Utils.TimeOutReason, ArrayList<Pair<Cancellable, Integer>>> entry : this.entrySet())
		{
			sb.append("[ ").append(entry.getKey());
			for (Pair<Cancellable, Integer> element : entry.getValue())
			{
				sb.append(", ").append(element.first().isCancelled()? "cancelled" : "active");
			}
			sb.append(" ]");
		}
		return sb.toString();
	}
}
