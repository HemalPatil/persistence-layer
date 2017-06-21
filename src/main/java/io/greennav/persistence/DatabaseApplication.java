package io.greennav.persistence;

import de.topobyte.osm4j.core.model.impl.Node;
import de.topobyte.osm4j.core.model.impl.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Created by hadoopuser on 15/6/17.
 */

// TODO: add logs
public class DatabaseApplication
{
	private static SparkSession sparkSession = null;
	private static SQLContext sqlContext = null;

	private static Persistence persistence = null;

	public static void runApplication(String appName, String masterUrl)
	{
		sparkSession = SparkSession.builder()
				.master(masterUrl)
				.appName(appName)
				.getOrCreate();
		sqlContext = sparkSession.sqlContext();
		persistence = new Persistence(sqlContext);
	}

	public static Persistence getPersistence()
	{
		return persistence;
	}



	public static void main(String[] args)
	{
		DatabaseApplication.runApplication("io.greennav.persistence.DatabaseApplication","local");

//		Dataset<Row> df = sqlContext.read().
//				parquet(DatabaseApplication.class.getClassLoader().getResource("berlin-latest.osm.pbf.node.parquet").getFile());
//		df.printSchema();
//		df.createOrReplaceTempView("te");
//
//		Dataset<Row> x = sqlContext.sql("SELECT id, CAST(tag.key AS STRING) AS key, CAST(tag.value AS STRING) AS value FROM (SELECT id, explode(tags) AS tag FROM te WHERE id=172548)");
//		x.show();
//		x.printSchema();
//		List<Row> y = x.collectAsList();
//		for(Row a : y)
//		{
//			System.out.println(a.getLong(a.fieldIndex("id")) + a.getString(a.fieldIndex("key")) + a.getString(a.fieldIndex("value")));
//		}
//		Node b;
//		Tag c;

		Persistence x = DatabaseApplication.getPersistence();
		try
		{
			System.out.println(x.getNodeById(172540).getLongitude());
		}
		catch (Exception e)
		{
			System.out.println("No such node");
		}
	}
}
