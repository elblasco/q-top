package it.unitn.disi.ds1.qtop;

import akka.actor.Cancellable;
import akka.japi.Pair;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;

public class TimeOutManager extends EnumMap<Utils.TimeOutReason, ArrayList<Pair<Cancellable, Integer>>> {
	// Phony map to associate a reason with its specific refresh ratio
	private final EnumMap<Utils.TimeOutReason, Integer> customTimeouts;
	private final int refresh;

	public TimeOutManager(int decisionTimeout, int voteTimeout, int heartbeatTimeout, int writeTimeout, int refresh) {
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
		this.refresh = refresh;
		for (Utils.TimeOutReason reason : Utils.TimeOutReason.values())
		{
			this.put(
					reason,
					new ArrayList<>()
			);
		}
	}

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
		this.get(reason).set(
				i,
				new Pair<>(
				action,
				this.customTimeouts.get(reason)
		));
		//System.out.println("reason: " + reason + " with index " + i + " in " + this);
	}

	/**
	 * Decrease the time left for a specific count down
	 *
	 * @param reason
	 * @param i
	 * @param node
	 * @param logger
	 */
	public void handleCountDown(Utils.TimeOutReason reason, int i, Node node, Logger logger) {
		//System.out.println("node: " + node.nodeId + " reason: " + reason + " with index " + i + " in " + this);
		if (this.get(reason).get(i).second() <= 0)
		{
			this.get(reason).get(i).first().cancel();
			node.tell(
					node.getSelf(),
					new Utils.TimeOut(reason),
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
			logger.log(
					Utils.LogLevel.DEBUG,
					"[NODE-" + node.nodeId + "] has not received " + reason + " yet, " + this.get(reason).get(i).second() + " ms left"
			);
		}
	}

	/**
	 * Reset a specific count down
	 *
	 * @param reason
	 * @param i
	 * @param nodeId
	 * @param logger
	 *
	 * @return
	 */
	public boolean resetCountDown(Utils.TimeOutReason reason, int i, int nodeId, Logger logger) {
//		logger.log(
//				Utils.LogLevel.INFO,
//				"[NODE-" + nodeId + "] is resetting " + i + "th countdown for " + reason
		//		);
		boolean ret;
		switch (reason)
		{
			case HEARTBEAT: // in the HEARTBEAT case the countdown is reset to its max
				this.get(reason).set(
						i,
						new Pair<>(
								this.get(reason).get(i).first(),
								this.customTimeouts.get(reason)
						)
				);
				ret = true;
				break;
			default: // in all other cases the timeout countdown is cancelled
				ret = this.get(reason).get(i).first().cancel();
				break;
		}
		return ret;
	}

	public void startElectionState() {
		//System.out.println(this);
		for (Map.Entry<Utils.TimeOutReason, ArrayList<Pair<Cancellable, Integer>>> entry : this.entrySet())
		{
			if (entry.getKey() != Utils.TimeOutReason.ELECTION)
			{
				for (Pair<Cancellable, Integer> element : entry.getValue())
				{
					element.first().cancel();
				}
			}
		}
		//System.out.println(this);
		/*try
		{
			Thread.sleep(5000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}*/
	}

	public void endElectionState() {
		// TODO cancel Election ACK and ripristinare all the other timeouts
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
