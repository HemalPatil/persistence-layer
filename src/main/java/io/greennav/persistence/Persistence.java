package io.greennav.persistence;
//
//import de.topobyte.osm4j.core.model.impl.Node;
//import de.topobyte.osm4j.core.model.impl.Tag;
//import org.apache.log4j.Logger;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SQLContext;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.List;
//
/**
 * Created by Hemal on 20-Jun-17.
 */
//public class Persistence implements IPersistence
//{
//	private static SQLContext sqlContext;
//
//	private static Dataset<Row> nodesPatch = null;
//	private static Dataset<Row> waysPatch = null;
//	private static Dataset<Row> relationsPatch = null;
//
//	private static double nodeCacheMinLatitude = -1000;
//	private static double nodeCacheMaxLatitude = 1000;
//	private static double nodeCacheMinLongitude = -1000;
//	private static double nodeCacheMaxLongitude = 1000;
//	private static List<Node> nodeCache = null;
//
//	private static Persistence p = null;
//
//	private static Logger logger;
//
//	public static Persistence getInstance(SQLContext sqlContext, String parquetDir, Logger l)
//	{
//		if(p == null)
//		{
//			Persistence.sqlContext = sqlContext;
//			Persistence.logger = l;
//
//			p = new Persistence();
//			logger.info("Persistence instance created");
//
//			if(parquetDir.charAt(parquetDir.length() - 1) != '/')
//			{
//				parquetDir += '/';
//			}
//			nodesPatch = sqlContext.read().parquet(parquetDir + "nodes.parquet");
//			nodesPatch.createOrReplaceTempView("nodes");nodesPatch.show();
//
//			waysPatch = sqlContext.read().parquet(parquetDir + "ways.parquet");
//			waysPatch.createOrReplaceTempView("ways");
//
//			relationsPatch = sqlContext.read().parquet(parquetDir + "relations.parquet");
//			relationsPatch.createOrReplaceTempView("relations");
//		}
//		return p;
//	}
//
//	@Override
//	public Node getNodeById(long id)
//	{
//		logger.info("getNodeById: " + id);
//		Dataset<Row> n = nodesPatch.where("id=" + id).select("id", "longitude", "latitude");//sqlContext.sql("SELECT id, latitude, longitude FROM nodes WHERE id=" + id);
//		if(n.count() == 1)
//		{
//			double longitude = 0, latitude = 0;
//			for(Row nrow : n.collectAsList())
//			{
//				longitude = nrow.getDouble(nrow.fieldIndex("longitude"));
//				latitude = nrow.getDouble(nrow.fieldIndex("latitude"));
//			}
//			Dataset<Row> tag = sqlContext.sql("SELECT CAST(tag.key AS STRING) AS key, CAST(tag.value AS STRING) AS value FROM (SELECT explode(tags) AS tag FROM nodes WHERE id=" + id + ")");
//			List<Tag> tags = new ArrayList<>();
//			for(Row tabRow : tag.collectAsList())
//			{
//				tags.add(new Tag(tabRow.getString(tabRow.fieldIndex("key")), tabRow.getString(tabRow.fieldIndex("value"))));
//			}
//			return new Node(id, longitude, latitude, tags);
//		}
//		logger.info("Node with id: " + id + " not found");
//		return null;
//	}
//}

//public class Persistence implements IPersistence
//{
//	private static Persistence instance = null;
//
//	public void lavda()
//	{
//		(new Builder()).lasun();
//		new DatabaseApplication();
//	}
//
//	public class Builder
//	{
//		private void lasun()
//		{
//
//		}
//	}
//}
