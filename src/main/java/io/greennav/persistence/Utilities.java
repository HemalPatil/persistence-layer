package io.greennav.persistence;

import de.topobyte.osm4j.core.model.iface.OsmTag;
import de.topobyte.osm4j.core.model.impl.Node;

/**
 * Created by Hemal on 22-Jun-17.
 */
public class Utilities
{
	public static void printNode(Node n)
	{
		if(n == null)
		{
			return;
		}
		System.out.println("Node " + n.getId());
		System.out.println("\tlatitude: " + n.getLatitude());
		System.out.println("\tlongitude: " + n.getLongitude());
		System.out.println("\ttags:");
		for(OsmTag t : n.getTags())
		{
			System.out.println("\t\t" + t.getKey() + ": " + t.getValue());
		}
	}
}
