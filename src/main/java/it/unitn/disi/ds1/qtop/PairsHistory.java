package it.unitn.disi.ds1.qtop;

import akka.japi.Pair;

import java.util.ArrayList;

/**
 * PairsHistory class to store the history of the pairs and the final decision associated to it
 */
public class PairsHistory extends ArrayList<ArrayList<Pair<Integer, Utils.Decision>>> {
	public PairsHistory() {
		super();
	}

	/**
	 * Get the state of the given epoch and iteration.
	 *
	 * @param e          epoch
	 * @param i          iteration
	 * @param finalState final state to set
	 */
	public void setState(int e, int i, Utils.Decision finalState) {
		this.get(e).set(i,
				new Pair<>(
						this.get(e).get(i).first(),
						finalState
				)
		);
	}

	/**
	 * Read the last valid, i.e., committed, variable.
	 *
	 * @return the last valid variable, -1 in case none is set
	 */
	public int readValidVariable() {
		if (! this.isEmpty())
		{
			for (int x = this.size() - 1; x >= 0; x--)
			{
				// If the current epoch is still empty, we have to check the previous epoch, i.e., -2
				//int endOffSet = this.get(x).isEmpty() ? - 1 : - 2;
				for (int y = this.get(x).size() - 1; y >= 0; y--)
				{
					if (this.get(x).get(y) != null && this.get(x).get(y).second() == Utils.Decision.WRITEOK)
					{
						return this.get(x).get(y).first();
					}
				}
			}
		}
		return - 1;
	}

	/**
	 * Insert a new element in the history.
	 *
	 * @param e       epoch
	 * @param i       iteration
	 * @param element element to insert
	 */
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
						Utils.Decision.PENDING
				)
		);
	}

	/**
	 * Get the latest epoch and iteration.
	 *
	 * @return the latest epoch and iteration
	 */
	public Utils.EpochPair getLatest() {
		if (! this.isEmpty())
		{
			int latestEpoch = this.size() - 1;
			int latestIteration = (this.get(latestEpoch).isEmpty()) ? 0 : this.get(latestEpoch).size() - 1;
			return new Utils.EpochPair(
					latestEpoch,
					latestIteration
			);
		}
		return new Utils.EpochPair(-1, -1);
	}

	/**
	 * Get the epoch and iteration of the last commited transaction, by commited transaction we consider a
	 * transaction that has non-pending state.
	 *
	 * @return the latest epoch and iteration committed
	 */
	public Utils.EpochPair getLatestCommitted(){
		if (! this.isEmpty())
		{
			for (int i = this.size() - 1; i >= 0; i--)
			{
				if (! this.get(i).isEmpty())
				{
					for (int j = this.get(i).size() - 1; j >= 0; j--)
					{
						if (this.get(i).get(j).second() == Utils.Decision.WRITEOK || this.get(i).get(j)
								.second() == Utils.Decision.ABORT)
						{
							return new Utils.EpochPair(
									i,
									j
							);
						}
					}
				}
			}
		}
		return new Utils.EpochPair(-1, -1);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (ArrayList<Pair<Integer, Utils.Decision>> epoch : this)
		{
			sb.append("[ ");
			for (Pair<Integer, Utils.Decision> iteration : epoch)
			{
				sb.append(iteration.first()).append(" ").append(iteration.second()).append(", ");
			}
			sb.replace(sb.length() - 2, sb.length(), "");
			sb.append(" ]\n");
		}
		sb.replace(sb.length() - 1, sb.length(), "");
		return sb.toString();
	}

	public static void main(String[] args) {
		PairsHistory pairsHistory = new PairsHistory();
		pairsHistory.insert(0, 0, 1);
		pairsHistory.insert(0, 1, 2);
		pairsHistory.insert(0, 2, 3);
		pairsHistory.insert(1, 0, 4);
		pairsHistory.insert(1, 1, 5);
		pairsHistory.insert(1, 2, 6);
		pairsHistory.insert(2, 0, 7);
		pairsHistory.insert(2, 1, 8);
		pairsHistory.insert(2, 2, 9);
		pairsHistory.setState(0, 0, Utils.Decision.WRITEOK);
		System.out.println(pairsHistory);
		System.out.println("Last element in the history " + pairsHistory.getLatest());
		System.out.println("Last element committed, either WRITEOK or ABORT " + pairsHistory.getLatestCommitted());
		System.out.println("Last vaue committed with WRITEOK " + pairsHistory.readValidVariable());
		pairsHistory.setState(2, 2, Utils.Decision.ABORT);
		System.out.println(pairsHistory);
		System.out.println("Last element in the history " + pairsHistory.getLatest());
		System.out.println("Last element committed, either WRITEOK or ABORT " + pairsHistory.getLatestCommitted());
		System.out.println("Last vaue committed with WRITEOK " + pairsHistory.readValidVariable());
	}
}