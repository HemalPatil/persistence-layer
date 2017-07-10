package io.greennav.persistence.importer;

import io.greennav.persistence.pbfparser.OsmFormat.StringTable;
import io.greennav.persistence.pbfparser.OsmFormat.Way;
import org.postgis.Point;

import java.util.HashMap;
import java.util.List;
import java.util.Stack;

/**
 * Created by Hemal on 07-Jul-17.
 */
public class WayStore
{
	private Stack<List<Way>> wayStack = new Stack<>();
	private Stack<StringTable> stringTablesStack = new Stack<>();
	private Stack<Integer> granularityStack = new Stack<>();
	private Stack<Long> latitudeOffsetStack = new Stack<>();
	private Stack<Long> longitudeOffsetStack = new Stack<>();
	private HashMap<Long, Point> globalNodeStore;
	private boolean stopWayProcessors = false;
	private int capacity = 50;
	private int count = 0;

	WayStore(int capacity)
	{
		this.capacity = capacity;
	}

	public synchronized void end()
	{
		System.out.println("Ending way processing");
		stopWayProcessors = true;
		notifyAll();
	}

	public void setNodeStore(List<HashMap<Long, Point>> nodeStores)
	{
		int totalSize = 0;
		for(int i = 0; i < nodeStores.size(); ++i)
		{
			totalSize += nodeStores.get(i).size();
		}
		globalNodeStore = new HashMap<>(totalSize);
		for(int i = 0; i < nodeStores.size(); ++i)
		{
			globalNodeStore.putAll(nodeStores.get(i));
		}
	}

	public HashMap<Long, Point> getNodeStore()
	{
		return globalNodeStore;
	}

	// TODO: replace souts by logs
	public synchronized Object[] get(int number)
	{
//		System.out.println("entered " + number);
		while(wayStack.empty())
		{
			try
			{
				if(stopWayProcessors)
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
		return new Object[]{wayStack.pop(), stringTablesStack.pop(), granularityStack.pop(), latitudeOffsetStack.pop(), longitudeOffsetStack.pop()};
	}

	public synchronized void put(List<Way> w, StringTable s, Integer granularity, Long latitudeOffset, Long longitudeOffset)
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
		wayStack.push(w);
		stringTablesStack.push(s);
		granularityStack.push(granularity);
		latitudeOffsetStack.push(latitudeOffset);
		longitudeOffsetStack.push(longitudeOffset);
		notifyAll();
	}
}
