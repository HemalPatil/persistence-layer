package io.greennav.persistence;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

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
				parquet(DatabaseApplication.class.getClassLoader().getResource("berlin-latest.osm.pbf.relation.parquet").getFile());
		df.printSchema();
	}
}
