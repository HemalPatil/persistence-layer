package io.greennav.persistence.importer;

import io.greennav.persistence.pbfparser.OsmFormat.DenseNodes;
import io.greennav.persistence.pbfparser.OsmFormat.StringTable;
import javafx.util.Pair;

import java.util.Stack;

/**
 * Created by Hemal on 06-Jul-17.
 */
public class DenseNodeStore
{
	private static Stack<DenseNodes> denseNodesStack = new Stack<>();
	private static Stack<StringTable> stringTablesStack = new Stack<>();

	public synchronized Pair<DenseNodes, StringTable> get()
	{
		while(denseNodesStack.empty())
		{
			try
			{
				if(Import.stopDenseNodeProcessors)
				{
					notifyAll();
					return null;
				}
				wait();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		return new Pair<>(denseNodesStack.pop(), stringTablesStack.pop());
	}

	public synchronized void put(DenseNodes d, StringTable s)
	{
		denseNodesStack.push(d);
		stringTablesStack.push(s);
		notifyAll();
	}
}
