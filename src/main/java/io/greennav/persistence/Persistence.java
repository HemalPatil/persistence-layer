package io.greennav.persistence;

import de.topobyte.osm4j.core.model.iface.EntityType;
import de.topobyte.osm4j.core.model.impl.*;
import de.topobyte.osm4j.core.model.util.OsmModelUtil;
import gnu.trove.list.array.TLongArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.postgis.LineString;
import org.postgis.PGgeometry;
import org.postgis.Point;
import scala.collection.Seq;

import java.sql.*;
import java.util.*;

/**
 * Created by hemal on 15/6/17.
 */

// TODO: add logs
public class Persistence implements IPersistence
{
	public enum CacheType
	{
		nodeCentric,
		includeAll
	}

	private static String protocol = "jdbc:postgresql";
	private static String host = "localhost:5432";
	private static String databaseName = "greennavdb";
	private static String user = "postgres";
	private static String password = "";
	private static boolean cacheEnabled = true;
	private static int queryLimit = 500;
	private static int cacheLimit = 100;
	private static double cacheRange = 100;
	private static CacheType cacheType = CacheType.nodeCentric;

	private static Connection connection = null;
	private static Statement s = null;

	private static HashMap<Long, Node> nodeCache = null;
	private static Node cacheCenter = null;

	private static Persistence persistence = null;
	private static Logger logger = null;

	private Persistence() {};

	public static Persistence getInstance(Logger x)
	{
		if(persistence == null)
		{
			persistence = new Persistence();
			logger = x;
			logger.info("Persistence instance created");
		}
		return persistence;
	}

	public Persistence host(String host)
	{
		Persistence.host = host;
		logger.info("Persistence host set to " + host);
		return this;
	}

	public Persistence databaseName(String name)
	{
		Persistence.databaseName = name;
		logger.info("Persistence database set to " + databaseName);
		return this;
	}

	public Persistence user(String user)
	{
		Persistence.user = user;
		logger.info("Persistence user set to " + user);
		return this;
	}

	public Persistence password(String password)
	{
		Persistence.password = password;
		logger.info("Persistence password set to " + password);
		return this;
	}

	public Persistence enableCache(boolean e)
	{
		Persistence.cacheEnabled = e;
		logger.info("Persistence cache enabled " + cacheEnabled);
		return this;
	}

	public Persistence queryLimit(int x)
	{
		Persistence.queryLimit = x;
		logger.info("Persistence query limit set to " + queryLimit);
		return this;
	}

	public Persistence cacheLimit(int x)
	{
		Persistence.cacheLimit = x;
		logger.info("Persistence cache limit set to " + cacheLimit);
		return this;
	}

	public Persistence cacheRange(double metres)
	{
		Persistence.cacheRange = metres;
		logger.info("Persistence cache range set to " + cacheRange);
		return this;
	}

	public Persistence cacheType(CacheType x)
	{
		Persistence.cacheType = x;
		return this;
	}

	public void connect() throws SQLException
	{
		nodeCache = new HashMap<>(cacheLimit);
		logger.info("Created node cache of size " + cacheLimit);
		String url = protocol + "://" + host + "/" + databaseName;
		logger.info("Trying to connect to database with " + url);
		connection = DriverManager.getConnection(url, user, password);
		logger.info("Connected to database");
		s = connection.createStatement();
	}

	private HashMap<Long, Node> resultSetToNodes(ResultSet r)
	{
		try
		{
			HashMap<Long, Node> nodes = new HashMap<>();
			while(r.next())
			{
				Point p = (Point) ((PGgeometry) r.getObject("way")).getGeometry();
				Long id = r.getLong("id");
				String[] tagArray = new String[] {};
				Array x = r.getArray("tags");
				if(x != null)
				{
					tagArray = (String[]) r.getArray("tags").getArray();
				}
				List<Tag> tags = new ArrayList<>(tagArray.length / 2);
				for(int i = 0; i < tagArray.length; i += 2)
				{
					tags.add(new Tag(tagArray[i], tagArray[i+1]));
				}
				nodes.put(id, new Node(id, p.x, p.y, tags));
			}
			return nodes;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Node getNodeById(long id)
	{
		if(cacheEnabled)
		{
			Node n = nodeCache.get(id);
			if (n != null)
			{
				System.out.println("Straight outta cache");
				return n;
			}
		}
		try
		{
			ResultSet r = s.executeQuery("SELECT id, tags, way FROM planet_osm_nodes WHERE id=" + id);
			if(r.isBeforeFirst())
			{
				Node n = resultSetToNodes(r).get(id);
				updateCache(n);
				return n;
			}
			// TODO: add no such node excepttion
//			else
//			{
//				Exception
//			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	private Collection<Node> _queryNodes(String nodeQuery)
	{
		try
		{
			ResultSet r = s.executeQuery(nodeQuery + " LIMIT " + queryLimit);
			if(r.isBeforeFirst())
			{
				return resultSetToNodes(r).values();
			}
			else
			{
				return new ArrayList<>(0);
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Collection<Node> queryNodes(String key, String value)
	{
		String nodeQuery = "SELECT id, n.tags AS tags, n.way AS way FROM planet_osm_point AS p" +
				" INNER JOIN planet_osm_nodes AS n ON p.osm_id=n.id WHERE p.tags @> '\"" +
				key + "\"=>\"" + value + "\"'::hstore";
		return _queryNodes(nodeQuery);
	}

	@Override
	public Collection<Node> queryNodesWithinRange(String key, String value, double longitude, double latitude, double metres)
	{
		if(cacheEnabled)
		{
			final List<Node> nodes = new ArrayList<>(cacheLimit / 2);
			double lat1 = latitude * Math.PI / 180.0;
			double lat2 = cacheCenter.getLatitude() * Math.PI / 180.0;
			double diff = (longitude - cacheCenter.getLongitude()) * Math.PI / 180.0;
			double dist = Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(diff);
			dist = Math.acos(dist) * 180 / Math.PI * 60 * 1.515 * 1609.344;
			if(dist < metres)
			{
				for(Node n : nodeCache.values())
				{
					Map<String, String> tagMap = OsmModelUtil.getTagsAsMap(n);
					if(tagMap.containsKey(key) && tagMap.get(key) == value)
					{
						nodes.add(n);
					}
				}
			}
		}
		String nodeQuery = "SELECT id, n.tags AS tags, n.way AS way FROM planet_osm_point AS p" +
				" INNER JOIN planet_osm_nodes AS n ON p.osm_id=n.id WHERE st_dwithin(geography(p.way), geography('POINT(" +
				longitude + " " + latitude + ")'), " + metres +
				") AND (p.tags @> '\"" + key + "\"=>\"" + value + "\"'::hstore)";
		return _queryNodes(nodeQuery);
	}

	@Override
	public Set<Node> getNeighbors(Node node)
	{
		try
		{
			ResultSet r = s.executeQuery("SELECT w.id AS id, w.nodes AS nodes, l.way AS way FROM planet_osm_ways AS w" +
					" INNER JOIN planet_osm_line AS l ON w.id=l.osm_id AND l.osm_id>=0 WHERE st_intersects(way," +
					" (SELECT way FROM planet_osm_point WHERE osm_id=" + node.getId() + "))");
			Set<Node> neighbors = new HashSet<>();
			while(r.next())
			{
				Long[] nodes = (Long[]) r.getArray("nodes").getArray();
				LineString lineString = (LineString) ((PGgeometry)r.getObject("way")).getGeometry();
				Point[] points = lineString.getPoints();
				int i = 0;
				for(Point p : points)
				{
					if(p.x == node.getLongitude() && p.y == node.getLatitude())
					{
						if(i-1 >= 0)
						{
							Node n1 = getNodeById(nodes[i-1]);
							neighbors.add(n1);
							System.out.println(n1.getId());
						}
						if(i+1 < nodes.length)
						{
							Node n2 = getNodeById(nodes[i+1]);
							neighbors.add(n2);
							System.out.println(n2.getId());
						}
					}
					++i;
				}
			}
			return neighbors;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	private void updateCache(Node n)
	{
		cacheCenter = n;
		ResultSet r;
		try
		{
//			switch (cacheType)
//			{
//				case nodeCentric:
//					break;
//				case includeAll:
//					break;
//			}
			r = s.executeQuery("SELECT id, tags, way FROM planet_osm_nodes" +
					" WHERE st_dwithin(geography(way), geography('POINT(" + n.getLongitude() + " " + n.getLatitude() + ")'), " +
					cacheRange + ") LIMIT " + cacheLimit + ";");
			nodeCache = resultSetToNodes(r);
			System.out.println("Cache has " + nodeCache.size() + " nodes");
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	private HashMap<Long, Way> resultSetToWays(ResultSet r)
	{
		try
		{
			HashMap<Long, Way> ways = new HashMap<>();
			while(r.next())
			{
				Long[] nodeList = (Long[]) r.getArray("nodes").getArray();
				TLongArrayList nodes = new TLongArrayList(nodeList.length);
				for(Long l : nodeList)
				{
					nodes.add(l);
				}
				String[] tagArray = (String[]) r.getArray(3).getArray();
				List<Tag> tags = new ArrayList<>(tagArray.length / 2);
				for(int i = 0; i < tagArray.length; i += 2)
				{
					tags.add(new Tag(tagArray[i], tagArray[i+1]));
				}
				Long id = r.getLong("id");
				ways.put(id, new Way(id, nodes, tags));
			}
			return ways;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Way getWayById(long id)
	{
		try
		{
			ResultSet r = s.executeQuery("SELECT id, nodes, tags FROM planet_osm_ways WHERE id=" + id);
			if(r.isBeforeFirst())
			{
				return resultSetToWays(r).get(id);
			}
		}
		catch (SQLException e)
		{
			System.out.println("Way does not exist");
			e.printStackTrace();
		}
		return null;
	}

	private Collection<Way> _queryEdges(String wayQuery)
	{
		try
		{
			ResultSet r = s.executeQuery(wayQuery + " LIMIT " + queryLimit);
			if(r.isBeforeFirst())
			{
				return resultSetToWays(r).values();
			}
			else
			{
				return new ArrayList<>(0);
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Collection<Way> queryEdges(String key, String value)
	{
		String wayQuery = "SELECT w.id as id, w.nodes as nodes, w.tags as tags FROM planet_osm_roads AS l" +
				" INNER JOIN planet_osm_ways AS w ON l.osm_id>=0 AND l.osm_id=w.id WHERE l.tags @> '\"" +
				key + "\"=>\"" + value + "\"'::hstore";
		return _queryEdges(wayQuery);
	}

	@Override
	public Collection<Way> queryEdgesWithinRange(String key, String value, double longitude, double latitude, double metres)
	{
		String wayQuery = "SELECT w.id as id, w.nodes as nodes, w.tags as tags FROM planet_osm_roads AS l" +
				" INNER JOIN planet_osm_ways AS w ON l.osm_id>=0 AND l.osm_id=w.id WHERE" +
				" st_dwithin(geography(way), geography('POINT(" + longitude + " " + latitude + ")'), " + metres +
				") AND l.tags @> '\"" +
				key + "\"=>\"" + value + "\"'::hstore";
		return _queryEdges(wayQuery);
	}

	private HashMap<Long, Relation> resultSetToRelations(ResultSet r)
	{
		try
		{
			HashMap<Long, Relation> relations = new HashMap<>();
			while(r.next())
			{
				String[] tagArray = (String[]) r.getArray(3).getArray();
				String[] memberArray = (String[]) r.getArray(2).getArray();
				List<Tag> tags = new ArrayList<>(tagArray.length / 2);
				List<RelationMember> members = new ArrayList<>(memberArray.length / 2);

				for(int i = 0; i < tagArray.length; i += 2)
				{
					tags.add(new Tag(tagArray[i], tagArray[i+1]));
				}

				for(int i = 0; i < memberArray.length; i += 2)
				{
					EntityType type = EntityType.Node;
					switch(memberArray[i].charAt(0))
					{
						case 'n':
							type = EntityType.Node;
							break;

						case 'w':
							type = EntityType.Way;
							break;

						case 'r':
							type = EntityType.Relation;
							break;
					}
					members.add(new RelationMember(Long.parseLong(memberArray[i].substring(1)), type, memberArray[i+1]));
				}
				Long id = r.getLong("id");
				relations.put(id, new Relation(id, members, tags));
			}
			return relations;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Relation getRelationById(long id)
	{
		try
		{
			ResultSet r = s.executeQuery("SELECT id, members, tags FROM planet_osm_rels WHERE id=" + id);
			if(r.isBeforeFirst())
			{
				return resultSetToRelations(r).get(id);
			}
		}
		catch (SQLException e)
		{
			System.out.println("Relation does not exist");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Collection<Relation> queryRelations(String key, String value)
	{
		try
		{
			ResultSet r = s.executeQuery("SELECT id, members, r.tags FROM planet_osm_line AS l" +
					" INNER JOIN planet_osm_rels AS r ON l.osm_id<0 AND -(l.osm_id)=r.id WHERE" +
					" l.tags @> '\"" + key + "\"=>\"" + value + "\"'::hstore");
			return resultSetToRelations(r).values();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args)
	{
		try
		{
			Persistence p = Persistence.getInstance(Logger.getLogger(DatabaseApplication.class.getName()))
					.databaseName("greennavdb")
					.user("greennav")
					.password("testpassword")
					.queryLimit(20);
			p.connect();
			p.getNeighbors(p.getNodeById(172539));
		}
		catch (SQLException e)
		{
			System.out.println("connection failed");
			e.printStackTrace();
		}
	}
}
