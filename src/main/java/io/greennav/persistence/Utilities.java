package io.greennav.persistence;

import de.topobyte.osm4j.core.model.iface.OsmRelationMember;
import de.topobyte.osm4j.core.model.iface.OsmTag;
import de.topobyte.osm4j.core.model.impl.Node;
import de.topobyte.osm4j.core.model.impl.Relation;
import de.topobyte.osm4j.core.model.impl.Way;
import gnu.trove.list.array.TLongArrayList;

/**
 * Created by Hemal on 22-Jun-17.
 */
public class Utilities
{
	public static int byteArrayToInt(byte[] bytes)
	{
		return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
	}

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
			System.out.println("\t\t" + t.getKey() + "=>" + t.getValue());
		}
	}

	public static void printWay(Way w)
	{
		if(w == null)
		{
			return;
		}
		System.out.println("Way " + w.getId());
		System.out.print("\tnodes:");
		TLongArrayList nodes = (TLongArrayList) w.getNodes();
		for(int i = 0; i < nodes.size(); ++i)
		{
			System.out.print(" " + nodes.get(i));
		}
		System.out.println("\n\ttags:");
		for(OsmTag t : w.getTags())
		{
			System.out.println("\t\t" + t.getKey() + "=>" + t.getValue());
		}
	}

	public static void printRelation(Relation r)
	{
		if(r == null)
		{
			return;
		}
		System.out.println("Relation " + r.getId());
		System.out.print("\tmembers: ");
		for(OsmRelationMember m : r.getMembers())
		{
			switch (m.getType())
			{
				case Node:
					System.out.print('n');
					break;
				case Way:
					System.out.print('w');
					break;
				case Relation:
					System.out.print('r');
					break;
			}
			System.out.print(m.getId() + " ");
		}
		System.out.println("\n\ttags:");
		for(OsmTag t : r.getTags())
		{
			System.out.println("\t\t" + t.getKey() + "=>" + t.getValue());
		}
	}
}
