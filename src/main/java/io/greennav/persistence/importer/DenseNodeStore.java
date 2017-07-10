package io.greennav.persistence.importer;

import io.greennav.persistence.pbfparser.OsmFormat.DenseNodes;
import io.greennav.persistence.pbfparser.OsmFormat.StringTable;

import java.util.Stack;

/**
 * Created by Hemal on 06-Jul-17.
 */
public class DenseNodeStore
{
	private Stack<DenseNodes> denseNodesStack = new Stack<>();
	private Stack<StringTable> stringTablesStack = new Stack<>();
	private Stack<Integer> granularityStack = new Stack<>();
	private Stack<Long> latitudeOffsetStack = new Stack<>();
	private Stack<Long> longitudeOffsetStack = new Stack<>();
	private boolean stopDenseNodeProcessors = false;
	private int capacity = 50;
	private int count = 0;

	DenseNodeStore(int capacity)
	{
		this.capacity = capacity;
	}

	public synchronized void end()
	{
		System.out.println("Ending dense node processing");
		stopDenseNodeProcessors = true;
		notifyAll();
	}

	// TODO: replace souts by logs
	public synchronized Object[] get(int number)
	{
//		System.out.println("entered " + number);
		while(denseNodesStack.empty())
		{
			try
			{
				if(stopDenseNodeProcessors)
				{
					notifyAll();
//					System.out.println("null " + number);
					return null;
				}
//				System.out.println("wait " + number);
				wait();
			}
			catch (InterruptedException e)
			{
//				System.out.println("get inter");
				e.printStackTrace();
			}
		}
//		System.out.println("return " + number);
		--count;
		notifyAll();
		return new Object[]{denseNodesStack.pop(), stringTablesStack.pop(), granularityStack.pop(), latitudeOffsetStack.pop(), longitudeOffsetStack.pop()};
	}

	public synchronized void put(DenseNodes d, StringTable s, Integer granularity, Long latitudeOffset, Long longitudeOffset)
	{
		while(count >= capacity)
		{
			try
			{
				System.out.println("full");
				wait();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		++count;
		denseNodesStack.push(d);
		stringTablesStack.push(s);
		granularityStack.push(granularity);
		latitudeOffsetStack.push(latitudeOffset);
		longitudeOffsetStack.push(longitudeOffset);
		notifyAll();
	}
}
