package io.greennav.persistence;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import de.topobyte.osm4j.core.model.impl.Entity;
import de.topobyte.osm4j.core.model.impl.Way;
import gnu.trove.list.array.TLongArrayList;
import io.greennav.persistence.pbfparser.FileFormat.Blob;
import io.greennav.persistence.pbfparser.FileFormat.BlobHeader;
import io.greennav.persistence.pbfparser.OsmFormat;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static io.greennav.persistence.Utilities.byteArrayToInt;

/**
 * Created by Hemal on 01-Jul-17.
 */
public class BlobParserTest
{
	public static void main(String[] args)
	{
		try
		{
			FileInputStream pbfFile = new FileInputStream("D:\\Hemal\\GSoC17\\berlin-latest.osm.pbf");
			byte[] blobSizeBytes = new byte[4];
			byte[] blobBytes;
			BlobHeader b;
			pbfFile.read(blobSizeBytes, 0, 4);
			int blobSize = byteArrayToInt(blobSizeBytes);
			blobBytes = new byte[blobSize];
			pbfFile.read(blobBytes, 0, blobSize);
			for(byte x : blobBytes)
			{
				System.out.print((char) x);
			}
			b = BlobHeader.parseFrom(blobBytes);
			if(b.getType().equals("OSMHeader"))
			{
				pbfFile.skip(b.getDatasize());
				System.out.println("Skipping " + b.getDatasize());
			}
			pbfFile.read(blobSizeBytes, 0, 4);
			blobSize = byteArrayToInt(blobSizeBytes);
			blobBytes = new byte[blobSize];
			pbfFile.read(blobBytes,0, blobSize);
			b = BlobHeader.parseFrom(blobBytes);
			System.out.println(b.getType() + " " + b.getDatasize());
			blobBytes = new byte[b.getDatasize()];
//			CodedInputStream cd = CodedInputStream.newInstance(pbfFile);
//			Blob x = Blob.parseFrom(cd);
			pbfFile.read(blobBytes,0, b.getDatasize());
			Blob x = Blob.parseFrom(blobBytes);
			if(x.hasZlibData())
			{
				System.out.println("has zlib data");
				byte[] lolax = x.getZlibData().toByteArray();
				Inflater inflater = new Inflater();
				inflater.setInput(lolax);
				byte[] buffer = new byte[1024];
				ByteArrayOutputStream op = new ByteArrayOutputStream(x.getRawSize());
				while(!inflater.finished())
				{
					int count = inflater.inflate(buffer);
					op.write(buffer, 0, count);
				}
				OsmFormat.PrimitiveBlock pr = OsmFormat.PrimitiveBlock.parseFrom(op.toByteArray());
				System.out.println(pr.getPrimitivegroupCount());
				for(ByteString bs : pr.getStringtable().getSList())
				{
					System.out.println(new String(bs.toByteArray()));
				}
				System.out.println(pr.getPrimitivegroup(0).getNodesCount());
				System.out.println(pr.getPrimitivegroup(0).getWaysCount());
				System.out.println(pr.getPrimitivegroup(0).getRelationsCount());
				System.out.println(pr.getPrimitivegroup(0).getChangesetsCount());
				if(pr.getPrimitivegroup(0).hasDense())
				{
					System.out.println("dense");
				}
				List<Entity> p = new ArrayList<>();
				Way po = new Way(90, new TLongArrayList());p.add(po);
			}
//			CodedInputStream cd = CodedInputStream.newInstance(pbfFile);
//			cd.skipRawBytes(4);
//			//BlobHeader b = BlobHeader.parseFrom(cd);
//			System.out.println("lolz" + cd.readTag() + "lolz" + cd.readString() + "lolz");
//			ByteArrayOutputStream x;
//			OsmFormat.PrimitiveGroup df;
//			Blob.parseFrom(new byte[1024]);BlobHeader de;de.hasDatasize()
		}
		catch (FileNotFoundException e)
		{
			System.out.println("file not found");
			e.printStackTrace();
		}
		catch (IOException e)
		{
			System.out.println("read failed");
			e.printStackTrace();
		}
		catch (DataFormatException e)
		{
			System.out.println("inflate failed");
			e.printStackTrace();
		}
	}
}
