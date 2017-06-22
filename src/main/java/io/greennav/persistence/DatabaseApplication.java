package io.greennav.persistence;

//import org.apache.hadoop.fs.LocalFileSystem;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SparkSession;


import de.topobyte.osm4j.core.model.iface.EntityType;
import de.topobyte.osm4j.core.model.impl.*;
import gnu.trove.list.array.TLongArrayList;
import org.postgis.PGgeometry;
import org.postgis.Point;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static io.greennav.persistence.Utilities.printNode;

/**
 * Created by hemal on 15/6/17.
 */

// TODO: add logs
public class DatabaseApplication implements IPersistence
{
//	private static SparkSession sparkSession = null;
//	private static SQLContext sqlContext = null;
//
//	private static Persistence persistence = null;
//	private static Logger logger = Logger.getLogger(DatabaseApplication.class.getName());
//
//	public static void runApplication(String appName, String master, String parquetDir)
//	{
//		//DatabaseApplication.logger = l;
//		logger.info("Starting database application");
//		logger.info("Creating Spark configuration");
//		SparkConf x = new SparkConf();
//		x.set("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse");
//		SparkContext y = new SparkContext(master, appName, x);
//		y.hadoopConfiguration().set("fs.file.impl", LocalFileSystem.class.getName());
//		sparkSession = new SparkSession(y);
//		sqlContext = sparkSession.sqlContext();
//		persistence = Persistence.getInstance(sqlContext, parquetDir, logger);
//	}
//
//	public static Persistence getPersistence()
//	{
//		return persistence;
//	}
//
//	public static void main(String[] args)
//	{
////		for(int i = 0; i < args.length; ++i)
////		{
////			switch(args[i])
////			{
////				case "--log":
////					if(args[i+1].equals("stdout"))
////					{
////
////					}
////			}
////		}
//
////		DatabaseApplication.runApplication("io.greennav.persistence.DatabaseApplication","local", "/home/hadoopuser/winGSoC17");
//
////		Dataset<Row> df = sqlContext.read().
////				parquet(DatabaseApplication.class.getClassLoader().getResource("berlin-latest.osm.pbf.node.parquet").getFile());
////		df.printSchema();
////		df.createOrReplaceTempView("te");
////
////		Dataset<Row> x = sqlContext.sql("SELECT id, CAST(tag.key AS STRING) AS key, CAST(tag.value AS STRING) AS value FROM (SELECT id, explode(tags) AS tag FROM te WHERE id=172548)");
////		x.show();
////		x.printSchema();
////		List<Row> y = x.collectAsList();
////		for(Row a : y)
////		{
////			System.out.println(a.getLong(a.fieldIndex("id")) + a.getString(a.fieldIndex("key")) + a.getString(a.fieldIndex("value")));
////		}
////		Node b;
////		Tag c;
//
////		Persistence x = DatabaseApplication.getPersistence();
////		try
////		{
////			System.out.println(x.getNodeById(172539).getTag(0).getValue());
////			System.out.println(x.getNodeById(172540).getLongitude());
////			System.out.println(x.getNodeById(172541).getLongitude());
////			System.out.println(x.getNodeById(172542).getLongitude());
////		}
////		catch (Exception e)
////		{
////			e.printStackTrace();
////			System.out.println("No such node");
////		}


//	}

	private static String protocol = "jdbc:postgresql";
	private static String host = "localhost:5432";
	private static String databaseName = "greennavdb";
	private static String user = "postgres";
	private static String password = "";
	private static boolean cacheEnabled = false;

	private static Connection connection = null;
	private static Statement s = null;

	private static DatabaseApplication application = null;

	private DatabaseApplication() {};

	public static DatabaseApplication getInstance()
	{
		if(application == null)
		{
			application = new DatabaseApplication();
		}
		return application;
	}

	public DatabaseApplication host(String host)
	{
		DatabaseApplication.host = host;
		return this;
	}

	public DatabaseApplication databaseName(String name)
	{
		DatabaseApplication.databaseName = name;
		return this;
	}

	public DatabaseApplication user(String user)
	{
		DatabaseApplication.user = user;
		return this;
	}

	public DatabaseApplication password(String password)
	{
		DatabaseApplication.password = password;
		return this;
	}

	public DatabaseApplication enableCache(boolean e)
	{
		DatabaseApplication.cacheEnabled = e;
		return this;
	}

	public void runApplication() throws SQLException
	{
		String url = protocol + "://" + host + "/" + databaseName;
		connection = DriverManager.getConnection(url, user, password);
		s = connection.createStatement();
	}

	@Override
	public Node getNodeById(long id)
	{
		try
		{
			ResultSet r = s.executeQuery("SELECT * FROM planet_osm_point WHERE osm_id=" + id);
			if (r.next())
			{
				Point p = (Point) ((PGgeometry) r.getObject("way")).getGeometry();
				List<Tag> tags = null;
				r = s.executeQuery("SELECT tags FROM planet_osm_nodes WHERE id=" + id);
				while(r.next())
				{
					String[] tagArray = (String[]) r.getArray(1).getArray();
					tags = new ArrayList<>(tagArray.length / 2);
					for(int i = 0; i < tagArray.length; i += 2)
					{
						tags.add(new Tag(tagArray[i], tagArray[i+1]));
					}
				}
				return new Node(id, p.x, p.y, tags);
			}
		}
		catch (SQLException e)
		{
			System.out.println("Node does not exist");
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
			if(r.next())
			{
				TLongArrayList nodes = new TLongArrayList((long[])r.getArray(2).getArray());
				String[] tagArray = (String[]) r.getArray(3).getArray();
				List<Tag> tags = new ArrayList<>(tagArray.length / 2);
				for(int i = 0; i < tagArray.length; i += 2)
				{
					tags.add(new Tag(tagArray[i], tagArray[i+1]));
				}
				return new Way(id, nodes, tags);
			}
		}
		catch (SQLException e)
		{
			System.out.println("Way does not exist");
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
			if(r.next())
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

				return new Relation(id, members, tags);
			}
		}
		catch (SQLException e)
		{
			System.out.println("Relation does not exist");
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args)
	{
		try
		{
			DatabaseApplication d = DatabaseApplication.getInstance()
					.host("10.0.2.15:5432")
					.databaseName("greennavdb")
					.user("greennav")
					.password("testpassword");
			d.runApplication();
			for(int i = 172539; i <= 200000; ++i)
			{
				printNode(d.getNodeById(i));
			}
		}
		catch (SQLException e)
		{
			System.out.println("connection failed");
			e.printStackTrace();
		}
	}
}
