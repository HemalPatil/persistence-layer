package io.greennav.persistence.importer;

import com.google.protobuf.CodedInputStream;
import de.topobyte.osm4j.core.model.impl.Node;
import io.greennav.persistence.pbfparser.FileFormat.Blob;
import io.greennav.persistence.pbfparser.FileFormat.BlobHeader;
import io.greennav.persistence.pbfparser.OsmFormat.*;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Created by Hemal on 03-Jul-17.
 */
public class Import
{
	public static final int BlobHeaderType = BlobHeader.TYPE_FIELD_NUMBER << 3 | 2;
	public static final int BlobHeaderIndexData = BlobHeader.INDEXDATA_FIELD_NUMBER << 3 | 2;
	public static final int BLobHeaderDatasize = BlobHeader.DATASIZE_FIELD_NUMBER << 3;

	public static final int BlobRaw = Blob.RAW_FIELD_NUMBER << 3 | 2;
	public static final int BlobRawSize = Blob.RAW_SIZE_FIELD_NUMBER << 3;
	public static final int BlobZlibData = Blob.ZLIB_DATA_FIELD_NUMBER << 3 | 2;
	public static final int BlobLzmaData = Blob.LZMA_DATA_FIELD_NUMBER << 3 | 2;
	public static final int BlobObsoleteData = Blob.OBSOLETE_BZIP2_DATA_FIELD_NUMBER << 3 | 2;

	private static Logger logger = Logger.getLogger(Import.class.getName());

	public static void importFile(String filePath) throws FileNotFoundException
	{
		FileInputStream pbfFile = new FileInputStream(filePath);
		CodedInputStream input = CodedInputStream.newInstance(pbfFile);
		//List<byte[]> compressedDataList = new ArrayList<>();
		Map<Long, Node> nodes = new HashMap<>();
		int headerCount = 0, dataCount = 0, nodeGroups = 0, wayGroups = 0, relationGrous = 0, changesetGroups = 0, denseGroups = 0;
		try
		{
			String url = "jdbc:postgresql://localhost:5432/pbftest";
			Connection connection = DriverManager.getConnection(url, "hemal", "roflolmao");
			Statement s = connection.createStatement();

			while(!input.isAtEnd())
			//for(int i = 0; i < 2; ++i)
			{
				input.skipRawBytes(4);
				input.readTag();
				String blobType = input.readString();
				if(input.readTag() == BlobHeaderIndexData)
				{
					input.readBytes();
					input.readTag();
				}
				int blobSize = input.readInt32();
				switch (blobType)
				{
					case "OSMHeader":
						++headerCount;
						System.out.println("Skipping over OSMHeader");
						input.skipRawBytes(blobSize);
						continue;
					case "OSMData":
						++dataCount;
						break;
				}
				int blobRawSize = 0;
				boolean compressed = true;
				switch(input.readTag())
				{
					case BlobRaw:
						compressed = false;
						break;
					case BlobRawSize:
						blobRawSize = input.readInt32();
						compressed = true;
						break;
				}
				byte[] rawBytes;
				if(!compressed)
				{
					rawBytes = input.readByteArray();
				}
				else
				{
					int compressionType = input.readTag();
					byte[] compressedBytes = {};
					switch (compressionType)
					{
						case BlobZlibData:
							compressedBytes = input.readByteArray();
							//compressedDataList.add(blob);
							break;
						default:
							System.out.println("Unsupported compression, skipping over");
							input.skipField(compressionType);
							break;
					}
					Inflater inflater = new Inflater();
					inflater.setInput(compressedBytes);
					rawBytes = new byte[blobRawSize];
					try
					{
						inflater.inflate(rawBytes);
					}
					catch (DataFormatException e)
					{
						System.out.println("Decompression of OSM data failed. Skipping over this block");
						continue;
					}
					PrimitiveBlock pb = PrimitiveBlock.parseFrom(rawBytes);
					StringTable stringTable = pb.getStringtable();
					int granularity = pb.getGranularity();
					long latitudeOffset = pb.getLatOffset();
					long longitudeOffset = pb.getLonOffset();

					for(PrimitiveGroup g : pb.getPrimitivegroupList())
					{
						if(g.getNodesCount() > 0)
						{
//							System.out.println("Node group");
							++nodeGroups;
						}
						else if(g.getWaysCount() > 0)
						{
//							System.out.println("Way group");
							++wayGroups;
						}
						else if(g.getRelationsCount() > 0)
						{
//							System.out.println("Relation group");
							++relationGrous;
						}
						else if(g.getChangesetsCount() > 0)
						{
//							System.out.println("Changeset group");
							++changesetGroups;
						}
						else
						{
							System.out.println("Processing dense nodes group: " + (denseGroups + 1));
							DenseNodes d = g.getDense();
							List<Long> ids = d.getIdList();
							List<Long> latitudes = d.getLatList();
							List<Long> longitudes = d.getLonList();
							List<Integer> keyVals = d.getKeysValsList();
							int keyValIndex = 0;
							int nodeIndex = 0;
							long previousId = 0;
							long previousLat = 0;
							long previousLon = 0;
							for(long deltaId : ids)
							{
								long currentId = previousId + deltaId;
								long currentLat = previousLat + latitudes.get(nodeIndex);
								long currentLon = previousLon + longitudes.get(nodeIndex);
								Map<String, String> tags = new HashMap<>(10);
								if(keyVals.size() != 0)
								{
									String key, value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
									++keyValIndex;
									while(!value.equals(""))
									{
										key = value;
										key = key.replace("\"", "");
										key = key.replace("\'", "");
										value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
										++keyValIndex;
										value = value.replace("\"", "");
										value = value.replace("\'", "");
										tags.put(key, value);
										value = new String(stringTable.getS(keyVals.get(keyValIndex)).toByteArray());
										++keyValIndex;
									}
								}
								previousId = currentId;
								previousLat = currentLat;
								previousLon = currentLon;
								double actualLat = (latitudeOffset + (granularity * currentLat)) * 0.000000001;
								double actualLon = (longitudeOffset + (granularity * currentLon)) * 0.000000001;
								DecimalFormat df = new DecimalFormat("##.#######");
								df.setRoundingMode(RoundingMode.DOWN);
								String lat = df.format(actualLat);
								String lon = df.format(actualLon);
								StringBuilder insertQuery = new StringBuilder("INSERT INTO planet_osm_nodes(id, lat, lon, tags, way) VALUES " +
										"(" + currentId + ", " + lat + ", " + lon + ", '");
								int index = 0, size = tags.size();
								for(Map.Entry<String, String> tag : tags.entrySet())
								{
									insertQuery.append("\"" + tag.getKey() + "\"=>\"" + tag.getValue() + "\"");
									if(index < size - 1)
									{
										insertQuery.append(", ");
									}
									++index;
								}
								insertQuery.append("', st_setsrid(st_makepoint(" + lat + ", " + lon + "), 4326))");
								s.addBatch(insertQuery.toString());
								++nodeIndex;
								if(nodeIndex % 2000 == 0)
								{
									s.executeBatch();
								}
							}
							s.executeBatch();
							System.out.println("Processed " + nodeIndex + " nodes in the dense group");
							++denseGroups;
						}
					}
				}
				input.resetSizeCounter();
			}
			System.out.println("Headers: " + headerCount);
			System.out.println("Data blocks: " + dataCount);
			System.out.println("Node groups: " + nodeGroups);
			System.out.println("Way groups: " + wayGroups);
			System.out.println("Relation groups: " + relationGrous);
			System.out.println("Dense node groups: " + denseGroups);
			System.out.println("Changeset groups: " + changesetGroups);

//			SparkConf conf = new SparkConf().setAppName("io.greennav.persistence.importer.Import")
//					.setMaster("spark://localhost:7077");
//			SparkContext sc = new SparkContext(conf);
//			sc.hadoopConfiguration().set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//			sc.hadoopConfiguration().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			JavaSparkContext jsc = new JavaSparkContext(sc);
//			jsc.parallelize(compressedDataList).foreach(new VoidFunction<byte[]>()
//			{
//				@Override
//				public void call(byte[] bytes) throws Exception
//				{
//				}
//			});
		}
		catch (IOException e)
		{
			System.out.println("CodedInputStream isAtEnd failed");
			e.printStackTrace();
		}
		catch (SQLException e)
		{
			System.out.println("Connnection create failed");
			e.printStackTrace();
		}
	}

	public static void main(String[] args)
	{
		try
		{
			Import.importFile("/home/hadoopuser/winGSoC17/berlin-latest.osm.pbf");
		}
		catch (FileNotFoundException e)
		{
			System.out.println("File not found");
		}
	}
}
