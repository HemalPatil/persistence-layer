package io.greennav.persistence;

import de.topobyte.osm4j.core.model.impl.Node;
import de.topobyte.osm4j.core.model.impl.Relation;
import de.topobyte.osm4j.core.model.impl.Way;

import java.util.Collection;
import java.util.Set;

/**
 * Created by Hemal on 20-Jun-17.
 */
public interface IPersistence
{
//	public void writeNode(Node node);
//
//	public void writeWay(Way way);
//
//	public void writeRelation(Relation relation);

	Node getNodeById(long id);

	Way getWayById(long id);

	Relation getRelationById(long id);
//
//	public Collection<Node> queryNodes(String key, String value);
//
//	public Collection<Way> queryEdges(String key, String value);
//
//	public Collection<Relation> queryRelations(String key, String value);
//
//	public Set<Node> getNeighbors(Node node);
}
