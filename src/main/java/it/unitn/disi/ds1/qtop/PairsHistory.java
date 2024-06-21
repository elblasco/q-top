package it.unitn.disi.ds1.qtop;


import java.util.ArrayList;

public class PairsHistory extends ArrayList<ArrayList<Utils.Pair>> {
	public PairsHistory() {
		super();
	}

	public void insert(int e, int i, int element) {
		if (this.isEmpty() || this.size() < e)
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
			for (int j = 0; j < (i - initialSize); j++)
			{
				this.get(e).add(null);
			}
		}
		this.get(e).add(new Utils.Pair(
				element,
				false
		));
	}

	public void setStateToTrue(int e, int i) {
		this.get(e).set(i, new Utils.Pair(this.get(e).get(i).number(), true));
	}

	public int readValidVariable() {
		if (!this.isEmpty()){
			for(int x = this.size() - 1; x >= 0; x--) {
				if(!this.get(x).isEmpty()){
					for (int y = this.get(x).size() - 1; y >= 0; y--)
					{
						if (this.get(x).get(y) != null && this.get(x).get(y).toWrite())
							return this.get(x).get(y).number();
					}
				}
			}
		}
		return -1;
	}
}