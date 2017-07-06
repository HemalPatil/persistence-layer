package io.greennav.persistence.importer;

import io.greennav.persistence.pbfparser.OsmFormat;
import io.greennav.persistence.pbfparser.OsmFormat.DenseNodes;
import javafx.util.Pair;

/**
 * Created by Hemal on 06-Jul-17.
 */
public class DenseNodesProcessor extends Thread
{
	private DenseNodeStore store;
	private int number;
	private int groupNumber = 0;

	DenseNodesProcessor(DenseNodeStore store, int number)
	{
		super();
		this.store = store;
		this.number = number;
	}

	@Override
	public void run()
	{
		System.out.println("Thread " + number + " started");
		while(true)
		{
			Pair<DenseNodes, OsmFormat.StringTable> d = store.get();
			if(d == null)
			{
				break;
			}
			++groupNumber;
			System.out.println("Thread " + number + " got group " + groupNumber);
		}
		System.out.println("Thread " + number + " exiting");
	}
}
