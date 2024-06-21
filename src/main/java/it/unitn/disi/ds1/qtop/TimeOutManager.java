package it.unitn.disi.ds1.qtop;

import akka.actor.Cancellable;
import akka.japi.Pair;

import java.util.ArrayList;
import java.util.EnumMap;

public class TimeOutManager extends EnumMap<Utils.TimeOutReason, ArrayList<Pair<Cancellable, Integer>>> {
	// Phony map to associate a reason with its specific refresh ratio
	private final EnumMap<Utils.TimeOutReason, Integer> timeoutRefresh;
	private final int refresh;

	public TimeOutManager(int decisionTimeout, int voteTimeout,
			int heartbeatTimeout, int refresh) {
		super(Utils.TimeOutReason.class);
		this.timeoutRefresh = new EnumMap<>(Utils.TimeOutReason.class);
		timeoutRefresh.put(Utils.TimeOutReason.VOTE, voteTimeout / refresh);
		timeoutRefresh.put(Utils.TimeOutReason.HEARTBEAT, heartbeatTimeout / refresh);
		timeoutRefresh.put(Utils.TimeOutReason.DECISION, decisionTimeout / refresh);
		this.refresh = refresh;
	}

	public void startCountDown(Utils.TimeOutReason reason, Cancellable action, int i) {
		if (this.get(reason) == null || this.get(reason).size() < i) {
			int initialSize = this.get(reason).size();
			for (int x = 0; x < (i - initialSize); x++) {
				this.get(reason).add(new Pair<>(null, this.timeoutRefresh.get(reason)));
			}
		}
		this.get(reason).add(new Pair<>(action, this.timeoutRefresh.get(reason)));
	}

	public void handleCountDown(Utils.TimeOutReason reason, int i, int nodeId, Logger logger){
		if (this.get(reason).get(i).second() <= 0)
		{
			this.get(reason).get(i).first().cancel();
			logger.log(
					Utils.LogLevel.INFO,
					"[NODE-" + nodeId + "] coordinator HEARTBEAT timeout"
			);
		}
		else
		{
			logger.log(
					Utils.LogLevel.DEBUG,
					"[NODE-" + nodeId + "] received HEARTBEAT from coordinator"
			);
			this.get(reason).set(i, new Pair<>(this.get(reason).get(i).first(),
					this.get(reason).get(i).second() - this.timeoutRefresh.get(reason)));
		}
	}
}
