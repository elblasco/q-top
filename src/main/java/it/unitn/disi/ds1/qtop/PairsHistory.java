package it.unitn.disi.ds1.qtop;


import akka.japi.Pair;

import java.util.ArrayList;

public class PairsHistory extends ArrayList<ArrayList<Pair<Integer, Boolean>>> {
	public PairsHistory() {
		super();
	}

	public void setStateToTrue(int e, int i) {
		this.get(e).set(i,
				new Pair<>(
						this.get(e).get(i).first(),
						true
				)
		);
	}

	public int readValidVariable() {
		if (! this.isEmpty())
		{
			for (int x = this.size() - 1; x >= 0; x--)
			{
				// If the current epoch is still empty, we have to check the previous epoch, i.e., -2
				int endOffSet = this.get(x).isEmpty() ? - 1 : - 2;
				for (int y = this.get(x).size() + endOffSet; y >= 0; y--)
				{
					if (this.get(x).get(y) != null && this.get(x).get(y).second())
					{
						return this.get(x).get(y).first();
					}
				}
			}
		}
		return - 15;
	}

	public void insert(int e, int i, int element) {
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
				this.get(e).add(null);
			}
		}
		this.get(e).set(
				i,
				new Pair<>(
				element,
				false
		));
	}

	public Pair<Integer, Integer> getLatest() {
		int latestEpoch = (this.isEmpty()) ? 0 : this.size() - 1;
		int latestIteration = (this.get(latestEpoch).isEmpty()) ? 0 : this.get(latestEpoch).size() - 1;
		return new Pair<>(
				latestEpoch,
				latestIteration
		);
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		for (ArrayList<Pair<Integer, Boolean>> epoch : this)
		{
			for (Pair<Integer, Boolean> iteration : epoch)
			{
				result.append(iteration.first()).append(" ").append(iteration.second()).append(", ");
			}
			result.append("\n");
		}
		return result.toString();
	}
}