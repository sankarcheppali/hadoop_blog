import org.apache.avro.*;
import org.apache.avro.file.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadAvroDataFile {

	public void readAvroFile(String srcUri) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs=FileSystem.get(conf);
		InputStream is=fs.open(new Path(srcUri));
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileStream<GenericRecord> dataFileStream =
				new DataFileStream<GenericRecord>(is, reader);
		GenericRecord record=null;
		while(dataFileStream.hasNext()){
			record=dataFileStream.next(record);
			System.out.println(record);
		}
		dataFileStream.close();
	}
	public void readAvroFile(String schemaUri,String srcUri) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs=FileSystem.get(conf);
		Schema.Parser parser = new Schema.Parser();
		Schema schema=parser.parse(fs.open(new Path(schemaUri)));
		InputStream is=fs.open(new Path(srcUri));
		DatumReader<GenericRecord> reader =
				new GenericDatumReader<GenericRecord>(null,schema);
		//reading schema is diff from writing schema
		DataFileStream<GenericRecord> dataFileStream =
				new DataFileStream<GenericRecord>(is, reader);
		GenericRecord record=null;
		while(dataFileStream.hasNext()){
			record=dataFileStream.next(record);
			System.out.println(record);
		}
		dataFileStream.close();
	}
	public static void main(String[] args) throws Exception {
		
		ReadAvroDataFile readAvroDataFile= new ReadAvroDataFile();
		if(args.length==1){
			readAvroDataFile.readAvroFile(args[0]);
		}
		else if(args.length==2)
		{
			readAvroDataFile.readAvroFile(args[0], args[1]);
		}
		else{
			System.out.println("yarn jar jarname ReadAvroDataFile [schema] filepath");
		}
	}

}
