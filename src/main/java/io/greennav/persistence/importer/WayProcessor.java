package io.greennav.persistence.importer;

import io.greennav.persistence.pbfparser.OsmFormat.StringTable;
import io.greennav.persistence.pbfparser.OsmFormat.Way;
import org.postgis.LineString;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.HStoreConverter;

import java.io.IOException;
import java.math.RoundingMode;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Hemal on 07-Jul-17.
 */
public class WayProcessor extends Thread
{
	private WayStore store;
	private Statement wayBatch;
	private Statement nodeStatement;
	private PreparedStatement wayPreparedStatement;
	private Connection connection;

	private int threadNumber;
	private int groupNumber = 0;

	WayProcessor(int number, WayStore store, String url, String user, String password)
	{
		super();
		this.store = store;
		this.threadNumber = number;
		try
		{
			this.connection = DriverManager.getConnection(url, user, password);
			wayBatch = connection.createStatement();
			nodeStatement = connection.createStatement();
			wayPreparedStatement = connection.prepareStatement("INSERT INTO planet_osm_ways (id, nodes, tags, way, distances) VALUES (?, ?, ?, ?, ?)");
		}
		catch (SQLException e)
		{
			System.out.println("Connection to database for way processor " + threadNumber + " failed");
			e.printStackTrace();
		}
	}

	// TODO replace souts by logs
	@Override
	public void run()
	{
		System.out.println("Way processor " + threadNumber + " started");
		List<Way> ways;
		StringTable stringTable;
		HashMap<Long, Point> nodeStore = store.getNodeStore();
//		Integer granularity;
//		Long latitudeOffset;
//		Long longitudeOffset;
//		Map<Long, Point> nodeCoords;
		while(true)
		{
			try
			{
				Object[] obj = store.get(threadNumber);
				if (obj == null)
				{
					break;
				}
				++groupNumber;
				ways = (List<Way>) obj[0];
				stringTable = (StringTable) obj[1];
//				granularity = (Integer) obj[2];
//				latitudeOffset = (Long) obj[3];
//				longitudeOffset = (Long) obj[4];
				System.out.println("Way processor " + threadNumber + " got group " + groupNumber);

				int size = ways.size();
				for(int i = 0; i < size;)
				{
//					Way w = ways.get(i);
//					List<Long> nodeList;




					Way w = ways.get(i);
					List<Long> nodeList = w.getRefsList();
					Point[] points = new Point[nodeList.size()];
					Long[] nodes = new Long[nodeList.size()];
					boolean needToFetch = false, firstFetch = true;
					StringBuilder query = new StringBuilder("SELECT id, way FROM planet_osm_nodes WHERE id IN (");
					long previousId = 0;
					int index = 0;
					List<Integer> notFound = new ArrayList<>();
					for(Long nodeId : nodeList)
					{
						long currentId = previousId + nodeId;
						if(nodeStore == null)
						{
							System.out.println("NODE STORE IS NULL");
						}
						Point p = nodeStore.get(currentId);
						if(p == null)
						{
							needToFetch = true;
							if(firstFetch)
							{
								firstFetch = false;
							}
							else
							{
								query.append(", ");
							}
							query.append(currentId);
							notFound.add(index);
						}
						else
						{
//							System.out.println("hit");
//							System.out.println(p.x + " " + p.y);
							points[index] = p;
						}
						nodes[index] = currentId;
						++index;
						previousId = currentId;
					}
					if(needToFetch)
					{
						query.append(")");
						ResultSet r = nodeStatement.executeQuery(query.toString());
						for(Integer j : notFound)
						{
							r.next();
							points[j] = (Point) ((PGgeometry)r.getObject("way")).getGeometry();
						}
					}
					List<Integer> keyIds = w.getKeysList();
					List<Integer> valueIds = w.getValsList();
					Map<String, String> tags = new HashMap<>(10);
					for(int j = 0; j < keyIds.size(); ++j)
					{
						String key = new String(stringTable.getS(keyIds.get(j)).toByteArray());
						String value = new String(stringTable.getS(valueIds.get(j)).toByteArray());
						tags.put(key, value);
					}
//					for(Point p : points)
//					{
//						System.out.println(p.x + " " + p.y);
//					}
					LineString way = new LineString(points);
//					System.out.println("len: " + way.length());
					way.setSrid(4326);
					Double[] distances = new Double[nodeList.size() - 1];
					for(int j = 1; j < points.length; ++j)
					{
						double lat1rad = points[j-1].y * Math.PI / 180;
						double lat2rad = points[j].y * Math.PI / 180;
						double lon1rad = points[j-1].x * Math.PI / 180;
						double lon2rad = points[j].x * Math.PI / 180;
						double deltaLatRad = lat1rad - lat2rad;
						double deltaLonRad = lon1rad - lon2rad;
						double a = Math.sin(deltaLatRad / 2) * Math.sin(deltaLatRad / 2)
								+ Math.cos(lat1rad ) * Math.cos(lat2rad) * Math.sin(deltaLonRad / 2) * Math.sin(deltaLonRad / 2);
						double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
						distances[j-1] = c * 6371000;
					}

					wayPreparedStatement.setLong(1, w.getId());
					wayPreparedStatement.setArray(2, connection.createArrayOf("int8", nodes));
					wayPreparedStatement.setObject(3, tags);
					wayPreparedStatement.setObject(4, new PGgeometry(way));
					wayPreparedStatement.setArray(5, connection.createArrayOf("float8", distances));
					wayPreparedStatement.addBatch();


//					StringBuilder insertQuery = new StringBuilder("INSERT INTO planet_osm_ways (id, nodes, tags, way, distances) VALUES (" + w.getId() + ", ARRAY[" + nodeList.get(0));
//					for(int j = 1; j < nodeList.size(); ++j)
//					{
//						long currentId = previousId + nodeList.get(j);
//						wayBatch.addBatch("UPDATE planet_osm_nodes SET ways = ways || CAST(" + w.getId() + " AS bigint) WHERE id=" + currentId);
//						query.append(", " + currentId);
//						insertQuery.append(", " + currentId);
////						System.out.println("way " + w.getId() + " node " + currentId);
//						previousId = currentId;
//					}
////					System.in.read();
//					query.append(")");
//					insertQuery.append("], '");
//					for(int j = 0; j < keyIds.size(); ++j)
//					{
//						String key = new String(stringTable.getS(keyIds.get(j)).toByteArray()).replace("\"", "");
//						key = key.replace("\'", "");
//						String value = new String(stringTable.getS(valueIds.get(j)).toByteArray()).replace("\"", "");
//						value = value.replace("\'", "");
////						System.out.println("way " + w.getId() + " " + key + "=>" + " " + value);
//						insertQuery.append("\"" + key + "\"=>\"" + value + "\"");
//						if(j < keyIds.size() -1)
//						{
//							insertQuery.append(", ");
//						}
//					}
////					System.in.read();
//					insertQuery.append("', ST_SETSRID(ST_MAKELINE(ARRAY[");
//					ResultSet r = nodeStatement.executeQuery(query.toString());
//					r.next();
//					Point p = (Point) ((PGgeometry)r.getObject("way")).getGeometry();
//					DecimalFormat df = new DecimalFormat("##.#######");
//					df.setRoundingMode(RoundingMode.DOWN);
//					String lat = df.format(p.y);
//					String lon = df.format(p.x);
//					insertQuery.append("st_makepoint(" + lon + ", " + lat + ")");
//					double previousLon = p.x;
//					double previousLat = p.y;
//					while(r.next())
//					{
//						p = (Point) ((PGgeometry)r.getObject("way")).getGeometry();
//						lat = df.format(p.y);
//						lon = df.format(p.x);
//						insertQuery.append(", st_makepoint(" + lon + ", " + lat + ")");
//
//						double lat1rad = previousLat * Math.PI / 180;
//						double lat2rad = p.y * Math.PI / 180;
//						double lon1rad = previousLon * Math.PI / 180;
//						double lon2rad = p.x * Math.PI / 180;
//						double deltaLatRad = lat1rad - lat2rad;
//						double deltaLonRad = lon1rad - lon2rad;
//						double a = Math.sin(deltaLatRad / 2) * Math.sin(deltaLatRad / 2)
//								+ Math.cos(lat1rad ) * Math.cos(lat2rad) * Math.sin(deltaLonRad / 2) * Math.sin(deltaLonRad / 2);
//						double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
//						distances.add(c * 6371000);
//
//						previousLat = p.y;
//						previousLon = p.x;
//					}
//					insertQuery.append("]), 4326), ARRAY[" + distances.get(0));
//					for(int j = 1; j < distances.size(); ++j)
//					{
//						insertQuery.append(", " + distances.get(j));
//					}
//					insertQuery.append("])");
//					wayBatch.addBatch(insertQuery.toString());
					++i;
					if(i%500 == 0)
					{
//						wayBatch.executeBatch();
						wayPreparedStatement.executeBatch();
						wayPreparedStatement.clearBatch();
					}
				}
//				wayBatch.executeBatch();
				wayPreparedStatement.executeBatch();
				wayPreparedStatement.clearBatch();
				System.out.println("way for loop end");
			}
			catch (SQLException e)
			{
				System.out.println("SQL batch failed for way processor " + threadNumber);
				e.printStackTrace();
			}
//			catch (IOException e)
//			{
//				e.printStackTrace();
//			}
		}
		System.out.println("way call end");
		store.end();
		System.out.println("Way processor " + threadNumber + " exiting after processing " + groupNumber + " way groups");
	}
}
