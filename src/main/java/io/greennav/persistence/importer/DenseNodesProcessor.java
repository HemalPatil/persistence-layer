package io.greennav.persistence.importer;

import io.greennav.persistence.pbfparser.OsmFormat.DenseNodes;
import io.greennav.persistence.pbfparser.OsmFormat.StringTable;
import org.postgis.PGgeometry;
import org.postgis.Point;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Hemal on 06-Jul-17.
 */
public class DenseNodesProcessor extends Thread
{
	private DenseNodeStore store;
	private PreparedStatement nodeBatch;
	private HashMap<Long, Point> nodeStore;

	private int threadNumber;
	private int groupNumber = 0;

	DenseNodesProcessor(int number, DenseNodeStore store, String url, String user, String password)
	{
		super();
		this.store = store;
		this.threadNumber = number;
		nodeStore = new HashMap<>();
		try
		{
			Connection connection = DriverManager.getConnection(url, user, password);
			nodeBatch = connection.prepareStatement("INSERT INTO planet_osm_nodes (id, lat, lon, tags, way) VALUES (?, ?, ?, ?, ?)");
		}
		catch (SQLException e)
		{
			System.out.println("Connection to database for dense nodes processor " + threadNumber + " failed");
			e.printStackTrace();
		}
	}

	public HashMap<Long, Point> getNodeStore()
	{
		return nodeStore;
	}

	// TODO replace souts by logs
	@Override
	public void run()
	{
		System.out.println("Dense nodes processor " + threadNumber + " started");
		DenseNodes d;
		StringTable stringTable;
		Integer granularity;
		Long latitudeOffset;
		Long longitudeOffset;
		while(true)
		{
			try
			{
				Object[] r = store.get(threadNumber);
				if (r == null)
				{
					break;
				}
				++groupNumber;
				d = (DenseNodes) r[0];
				stringTable = (StringTable) r[1];
				granularity = (Integer) r[2];
				latitudeOffset = (Long) r[3];
				longitudeOffset = (Long) r[4];
				System.out.println("Dense node processor " + threadNumber + " got group " + groupNumber);
				List<Long> ids = d.getIdList();
				List<Long> latitudes = d.getLatList();
				List<Long> longitudes = d.getLonList();
				List<Integer> keyVals = d.getKeysValsList();
				int keyValIndex = 0;
				int nodeIndex = 0;
				long previousId = 0;
				long previousLat = 0;
				long previousLon = 0;
				for (long deltaId : ids)
				{
					long currentId = previousId + deltaId;
					long currentLat = previousLat + latitudes.get(nodeIndex);
					long currentLon = previousLon + longitudes.get(nodeIndex);
					Map<String, String> tags = new HashMap<>(10);
					if (keyVals.size() != 0)
					{
						String key, value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
						++keyValIndex;
						while (!value.equals(""))
						{
							key = value;
							value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
							++keyValIndex;
							tags.put(key, value);
							value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
							++keyValIndex;
						}
					}
					//System.out.println(currentId);
					//System.in.read();
					previousId = currentId;
					previousLat = currentLat;
					previousLon = currentLon;
					double actualLat = (latitudeOffset + (granularity * currentLat)) * 0.000000001;
					double actualLon = (longitudeOffset + (granularity * currentLon)) * 0.000000001;
					nodeBatch.setLong(1, currentId);
					nodeBatch.setDouble(2, actualLat);
					nodeBatch.setDouble(3, actualLon);
					nodeBatch.setObject(4, tags);
					Point pg = new Point(actualLon, actualLat);
					pg.setSrid(4326);
					//nodeStore.put(currentId, pg);
					nodeBatch.setObject(5, new PGgeometry(pg));
					nodeBatch.addBatch();
					++nodeIndex;
					if (nodeIndex % 4000 == 0)
					{
						nodeBatch.executeBatch();
						nodeBatch.clearBatch();
					}
				}
				nodeBatch.executeBatch();
				nodeBatch.clearBatch();
				System.out.println("Dense nodes processor " + threadNumber + " processed " + nodeIndex + " nodes in the dense group");
			}
			catch (SQLException e)
			{
				System.out.println("SQL batch failed for dense nodes processor " + threadNumber);
				e.printStackTrace();
			}
//			catch (IOException e)
//			{
//				e.printStackTrace();
//			}
		}
		store.end();
		System.out.println("NODE PROC CACHE " + nodeStore.size());
		System.out.println("Dense nodes processor " + threadNumber + " exiting after processing " + groupNumber + " of dense nodes groups");
	}
}
