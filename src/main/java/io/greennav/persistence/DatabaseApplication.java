package io.greennav.persistence;

import de.topobyte.osm4j.core.model.impl.Node;
import de.topobyte.osm4j.core.model.impl.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by hadoopuser on 15/6/17.
 */

public class DatabaseApplication
{
	private static SQLContext sqlContext;
	public static void main(String[] args)
	{
		sqlContext = SparkContextResource.getSQLContext();
		Dataset<Row> df = sqlContext.read().
				parquet(DatabaseApplication.class.getClassLoader().getResource("berlin-latest.osm.pbf.node.parquet").getFile());
		df.printSchema();
		df.createOrReplaceTempView("te");

		Dataset<Row> x = sqlContext.sql("SELECT id, CAST(tag.key AS STRING) AS key, CAST(tag.value AS STRING) AS value FROM (SELECT id, explode(tags) AS tag FROM te WHERE id=172548)");
		x.show();
		x.printSchema();
		List<Row> y = x.collectAsList();
		for(Row a : y)
		{
			System.out.println(a.getLong(a.fieldIndex("id")) + a.getString(a.fieldIndex("key")) + a.getString(a.fieldIndex("value")));
		}
		Node b;
		Tag c;
	}
}
