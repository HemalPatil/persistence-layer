package io.greennav.persistence;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import io.greennav.persistence.pbfparser.FileFormat.Blob;
import io.greennav.persistence.pbfparser.FileFormat.BlobHeader;
import io.greennav.persistence.pbfparser.OsmFormat.*;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

	public static void importFile(String filePath) throws FileNotFoundException
	{
		FileInputStream pbfFile = new FileInputStream(filePath);
		CodedInputStream input = CodedInputStream.newInstance(pbfFile);
		//List<byte[]> compressedDataList = new ArrayList<>();
		int headerCount = 0, dataCount = 0, nodeGroups = 0, wayGroups = 0, relationGrous = 0, changesetGroups = 0, denseGroups = 0;
		try
		{
			while(!input.isAtEnd())
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
				byte[] rawBytes = {};
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
							System.out.println(dataCount);
							compressedBytes = input.readByteArray();
							System.out.println("done till here");
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
//							System.out.println("Dense nodes group");
							DenseNodes d = g.getDense();
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

//			SparkConf conf = new SparkConf().setAppName("io.greennav.persistence.Import")
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
	}

	public static void main(String[] args)
	{
		try
		{
			Import.importFile("D:\\Hemal\\GSoC17\\monaco-latest.osm.pbf");
		}
		catch (FileNotFoundException e)
		{
			System.out.println("File not found");
		}
	}
}
