package it.unitn.disi.ds1.qtop;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class that represents/handle the voters map.
 */
public class VotersMap extends ArrayList<ArrayList<Utils.VotePair>> {

	/**
	 * VotersMap constructor.
	 */
	public VotersMap() {
		super();
	}

	/**
	 * Insert a vote in the map.
	 *
	 * @param e        the epoch
	 * @param i        the index
	 * @param actorRef the actor reference
	 * @param vote     the vote
	 */
	public void insert(int e, int i, ActorRef actorRef, Utils.Vote vote) {
		if (this.isEmpty() || this.size() <= e)
		{
			int initialSize = this.size();
			for (int j = 0; j <= (e - initialSize); j++)
			{
				this.add(new ArrayList<>());
			}
		}
		if (this.get(e).isEmpty() || this.get(e).size() <= i)
		{
			int initialSize = this.get(e).size();
			for (int j = 0; j <= (i - initialSize); j++)
			{
				this.get(e).add(new Utils.VotePair(
						new HashMap<>(),
						Utils.Decision.PENDING
				));
			}

		}
		this.get(e).get(i).votes().put(
				actorRef,
				vote
		);

	}

	/**
	 * Set the decision for a specific epoch and index.
	 *
	 * @param d the decision
	 * @param e the epoch
	 * @param i the index
	 */
	public void setDecision(Utils.Decision d, int e, int i) {
		this.get(e).set(i,
				new Utils.VotePair(
						this.get(e).get(i).votes(),
						d
				)
		);
	}
}
