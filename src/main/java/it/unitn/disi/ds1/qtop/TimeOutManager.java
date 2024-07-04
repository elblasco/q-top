package it.unitn.disi.ds1.qtop;

import akka.actor.Cancellable;
import akka.japi.Pair;

import java.util.ArrayList;
import java.util.EnumMap;

public class TimeOutManager extends EnumMap<Utils.TimeOutReason, ArrayList<Pair<Cancellable, Integer>>> {
	// Phony map to associate a reason with its specific refresh ratio
	private final EnumMap<Utils.TimeOutReason, Integer> customTimeouts;
	private final int refresh;

	public TimeOutManager(int decisionTimeout, int voteTimeout,
			int heartbeatTimeout, int refresh) {
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
				Utils.TimeOutReason.DECISION,
				decisionTimeout
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
		if (this.get(reason).size() < i)
		{
			int initialSize = this.get(reason).size();
			for (int x = 0; x < (i - initialSize); x++) {
				// Padding of empty pairs
				this.get(reason).add(new Pair<>(
						null,
						this.customTimeouts.get(reason)
				));
			}
		}
		this.get(reason).add(new Pair<>(
				action,
				this.customTimeouts.get(reason)
		));
	}

	public void resetCountDown(Utils.TimeOutReason reason, int i, int nodeId, Logger logger) {
		logger.log(
				Utils.LogLevel.INFO,
				"[NODE-" + nodeId + "] is reseting " + i + "th countdown for " + reason
		);
		switch (reason)
		{
			case HEARTBEAT: // in the HEARTBEAT case the timer is reset to its max
				this.get(reason).set(
						i,
						new Pair<>(
								this.get(reason).get(i).first(),
								this.customTimeouts.get(reason)
						)
				);
			default: // in all other cases the timeout countdown is cancelled
				this.get(reason).get(i).first().cancel();
		}
	}

	public void handleCountDown(Utils.TimeOutReason reason, int i, int nodeId, Logger logger){
		if (this.get(reason).get(i).second() <= 0)
		{
			this.get(reason).get(i).first().cancel();
			logger.log(
					Utils.LogLevel.INFO,
					"[NODE-" + nodeId + "] " + reason + " timeout"
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
					"[NODE-" + nodeId + "] has not received " + reason + " yet, " + this.get(reason).get(i)
							.second() + " seconds left"
			);
		}
	}
}
