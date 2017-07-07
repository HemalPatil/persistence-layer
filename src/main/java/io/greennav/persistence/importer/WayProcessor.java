package io.greennav.persistence.importer;

import io.greennav.persistence.pbfparser.OsmFormat.StringTable;
import io.greennav.persistence.pbfparser.OsmFormat.Way;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Hemal on 07-Jul-17.
 */
public class WayProcessor extends Thread
{
	private WayStore store;
	private Statement s;

	private int threadNumber;
	private int groupNumber = 0;

	WayProcessor(int number, WayStore store, String url, String user, String password)
	{
		super();
		this.store = store;
		this.threadNumber = number;
		this.s = s;
		try
		{
			Connection connection = DriverManager.getConnection(url, user, password);
			s = connection.createStatement();
		}
		catch (SQLException e)
		{
			System.out.println("Connection to database for thread " + threadNumber + " failed");
			e.printStackTrace();
		}
	}

	// TODO replace souts by logs
	@Override
	public void run()
	{
		System.out.println("Thread " + threadNumber + " started");
		Way w;
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
				w = (Way) r[0];
				stringTable = (StringTable) r[1];
				granularity = (Integer) r[2];
				latitudeOffset = (Long) r[3];
				longitudeOffset = (Long) r[4];
				System.out.println("Thread " + threadNumber + " got group " + groupNumber);
//				List<Long> ids = w.getIdList();
//				List<Long> latitudes = w.getLatList();
//				List<Long> longitudes = w.getLonList();
//				List<Integer> keyVals = w.getKeysValsList();
//				int keyValIndex = 0;
//				int nodeIndex = 0;
//				long previousId = 0;
//				long previousLat = 0;
//				long previousLon = 0;
//				for (long deltaId : ids)
//				{
//					long currentId = previousId + deltaId;
//					long currentLat = previousLat + latitudes.get(nodeIndex);
//					long currentLon = previousLon + longitudes.get(nodeIndex);
//					Map<String, String> tags = new HashMap<>(10);
//					if (keyVals.size() != 0)
//					{
//						String key, value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
//						++keyValIndex;
//						while (!value.equals(""))
//						{
//							key = value;
//							key = key.replace("\"", "");
//							key = key.replace("\'", "");
//							value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
//							++keyValIndex;
//							value = value.replace("\"", "");
//							value = value.replace("\'", "");
//							tags.put(key, value);
//							value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
//							++keyValIndex;
//						}
//					}
//					previousId = currentId;
//					previousLat = currentLat;
//					previousLon = currentLon;
//					double actualLat = (latitudeOffset + (granularity * currentLat)) * 0.000000001;
//					double actualLon = (longitudeOffset + (granularity * currentLon)) * 0.000000001;
//					DecimalFormat df = new DecimalFormat("##.#######");
//					df.setRoundingMode(RoundingMode.DOWN);
//					String lat = df.format(actualLat);
//					String lon = df.format(actualLon);
//					StringBuilder insertQuery = new StringBuilder("INSERT INTO planet_osm_nodes(id, lat, lon, tags, way) VALUES " +
//							"(" + currentId + ", " + lat + ", " + lon + ", '");
//					int index = 0, size = tags.size();
//					for (Map.Entry<String, String> tag : tags.entrySet())
//					{
//						insertQuery.append("\"" + tag.getKey() + "\"=>\"" + tag.getValue() + "\"");
//						if (index < size - 1)
//						{
//							insertQuery.append(", ");
//						}
//						++index;
//					}
//					insertQuery.append("', st_setsrid(st_makepoint(" + lat + ", " + lon + "), 4326))");
//					s.addBatch(insertQuery.toString());
//					++nodeIndex;
//					if (nodeIndex % 4000 == 0)
//					{
//						s.executeBatch();
//					}
//				}
//				s.executeBatch();
//				System.out.println("Processed " + nodeIndex + " nodes in the dense group");
			}
			catch (SQLException e)
			{
				System.out.println("SQL batch failed");
				e.printStackTrace();
			}
		}
		store.end();
		System.out.println("Thread " + threadNumber + " exiting " + groupNumber);
	}
}
