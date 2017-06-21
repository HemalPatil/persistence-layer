package io.greennav.persistence;

import de.topobyte.osm4j.core.model.impl.Node;
import de.topobyte.osm4j.core.model.impl.Tag;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hemal on 20-Jun-17.
 */
public class Persistence implements IPersistence
{
	private SQLContext sqlContext;

	private static Dataset<Row> nodes = null;
	private static Dataset<Row> ways = null;
	private static Dataset<Row> relations = null;

	Persistence(SQLContext sqlContext)
	{
		super();
		this.sqlContext = sqlContext;

		nodes = sqlContext.read().parquet(DatabaseApplication.class.getClassLoader().getResource("berlin-latest.osm.pbf.node.parquet").getFile());
		nodes.createOrReplaceTempView("nodes");

		ways = sqlContext.read().parquet(DatabaseApplication.class.getClassLoader().getResource("berlin-latest.osm.pbf.way.parquet").getFile());
		ways.createOrReplaceTempView("ways");

		relations = sqlContext.read().parquet(DatabaseApplication.class.getClassLoader().getResource("berlin-latest.osm.pbf.relation.parquet").getFile());
		relations.createOrReplaceTempView("relations");
	}

	@Override
	public Node getNodeById(long id) throws Exception
	{
		Dataset<Row> n = sqlContext.sql("SELECT id, latitude, longitude FROM nodes WHERE id=" + id);
		if(n.count() == 1)
		{
			double longitude = 0, latitude = 0;
			for(Row nrow : n.collectAsList())
			{
				longitude = nrow.getDouble(nrow.fieldIndex("longitude"));
				latitude = nrow.getDouble(nrow.fieldIndex("latitude"));
			}
			Dataset<Row> tag = sqlContext.sql("SELECT id, CAST(tag.key AS STRING) AS key, CAST(tag.value AS STRING) AS value FROM (SELECT id, explode(tags) AS tag FROM te WHERE id=" + id + ")");
			List<Tag> tags = new ArrayList<>();
			for(Row tabRow : tag.collectAsList())
			{
				tags.add(new Tag(tabRow.getString(tabRow.fieldIndex("key")), tabRow.getString(tabRow.fieldIndex("value"))));
			}
			return new Node(id, longitude, latitude, tags);
		}
		return null;
	}
}
