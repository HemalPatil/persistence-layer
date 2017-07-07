package io.greennav.persistence.importer;

import com.google.protobuf.CodedInputStream;
import io.greennav.persistence.pbfparser.FileFormat.Blob;
import io.greennav.persistence.pbfparser.FileFormat.BlobHeader;
import io.greennav.persistence.pbfparser.OsmFormat.*;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

	public static void importFile(String filePath, int numberOfThreads) throws FileNotFoundException
	{
		FileInputStream pbfFile = new FileInputStream(filePath);
		CodedInputStream input = CodedInputStream.newInstance(pbfFile);
		DenseNodeStore store = new DenseNodeStore(50);
		DenseNodesProcessor[] denseNodesProcessors = new DenseNodesProcessor[numberOfThreads];
		int headerCount = 0, dataCount = 0, nodeGroups = 0, wayGroups = 0, relationGroups = 0, changesetGroups = 0, denseGroups = 0;
		try
		{
			String url = "jdbc:postgresql://localhost:5432/pbftest";
			for(int i = 0; i < numberOfThreads; ++i)
			{
				denseNodesProcessors[i] = new DenseNodesProcessor(i+1, store, url, "hemal", "roflolmao");
				denseNodesProcessors[i].start();
			}

			while(!input.isAtEnd())
//			for(int i = 0; i < 100; ++i)
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
				}
				PrimitiveBlock pb = PrimitiveBlock.parseFrom(rawBytes);
				StringTable stringTable = pb.getStringtable();
				Integer granularity = pb.getGranularity();
				Long latitudeOffset = pb.getLatOffset();
				Long longitudeOffset = pb.getLonOffset();
				for(PrimitiveGroup g : pb.getPrimitivegroupList())
				{
					if(g.getNodesCount() > 0)
					{
//						System.out.println("Node group");
						++nodeGroups;
					}
					else if(g.getWaysCount() > 0)
					{
//						System.out.println("Way group");
						++wayGroups;
					}
					else if(g.getRelationsCount() > 0)
					{
//						System.out.println("Relation group");
						++relationGroups;
					}
					else if(g.getChangesetsCount() > 0)
					{
//						System.out.println("Changeset group");
						++changesetGroups;
					}
					else
					{
						++denseGroups;
						System.out.println("Processing dense nodes group: " + denseGroups);
						DenseNodes d = g.getDense();
						//store.put(d, stringTable, granularity, latitudeOffset, longitudeOffset);
					}
				}
				input.resetSizeCounter();
			}
			System.out.println("Headers: " + headerCount);
			System.out.println("Data blocks: " + dataCount);
			System.out.println("Node groups: " + nodeGroups);
			System.out.println("Way groups: " + wayGroups);
			System.out.println("Relation groups: " + relationGroups);
			System.out.println("Dense node groups: " + denseGroups);
			System.out.println("Changeset groups: " + changesetGroups);
		}
		catch (IOException e)
		{
			System.out.println("CodedInputStream isAtEnd failed");
			e.printStackTrace();
		}
		store.end();
		System.out.println("stopped dense");
		for(int i = 0; i < numberOfThreads; ++i)
		{
			try
			{
				denseNodesProcessors[i].join();
			}
			catch (InterruptedException e)
			{
				System.out.println("Thread " + (i + 1) + " was interrupted");
			}
		}
		System.out.println("Dense nodes processing over");
	}

	public static void main(String[] args)
	{
		try
		{
			Import.importFile("/home/hadoopuser/winGSoC17/berlin-latest.osm.pbf", 4);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("File not found");
		}
	}
}
