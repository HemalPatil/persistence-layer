package io.greennav.persistence;

import com.google.protobuf.CodedInputStream;
import io.greennav.persistence.pbfparser.FileFormat.Blob;
import io.greennav.persistence.pbfparser.FileFormat.BlobHeader;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.master.Master;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
		List<byte[]> compressedDataList = new ArrayList<>();
		int headerCount = 0, dataCount = 0;
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
				if(compressed)
				{
					int compressionType = input.readTag();
					switch (compressionType)
					{
						case BlobZlibData:
							byte[] blob = input.readByteArray();
							compressedDataList.add(blob);
							break;
						default:
							System.out.println("Unsupported compression, skipping over");
							input.skipField(compressionType);
							break;
					}
				}
			}
			System.out.println("Headers: " + headerCount);
			System.out.println("Data blocks: " + dataCount);
			System.out.println("Total bytes read: " + input.getTotalBytesRead());

			SparkSession session = SparkSession.builder()
					.appName("io.greennav.persistence.Import")
					.master("spark://192.168.99.1:7077")
					.getOrCreate();
			System.out.println("Session initialized");
			JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
			sc.parallelize(compressedDataList).foreach(x -> System.out.println(x.length));
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
			Import.importFile("D:\\Hemal\\GSoC17\\berlin-latest.osm.pbf");
		}
		catch (FileNotFoundException e)
		{
			System.out.println("File not found");
		}
	}
}
