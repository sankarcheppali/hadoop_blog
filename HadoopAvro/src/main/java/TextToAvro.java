import org.apache.avro.*;
import org.apache.avro.file.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class TextToAvro {
    
    public void run(String[] args) throws Exception{
        String srcUri=args[0];
        String schemaUri=args[1];
        String dstUri=args[2];
        Configuration conf = new Configuration();
        FileSystem fs=FileSystem.get(conf);
        Schema.Parser parser = new Schema.Parser();
        Schema schema=parser.parse(fs.open(new Path(schemaUri)));
        InputStream is=fs.open(new Path(srcUri));
        BufferedReader br=new BufferedReader(new InputStreamReader(is,"UTF-8"));
        String line;
        String[] csvSchema;
          //Load CSV file column infromation, the schema should use same names
          if(( line=br.readLine())!=null){
              csvSchema=line.split(",");
          }
          else{
              return;
          }
        
        GenericRecord datum=new GenericData.Record(schema);
        DatumWriter<GenericRecord> writer =new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, fs.create(new Path(dstUri)));
        
          while(( line=br.readLine())!=null){
        	  String[] values=line.split(",");
        	  for(int i=0;i<values.length;i++){
        	      datum.put(csvSchema[i],values[i]);
        	  }
        	  
        	  dataFileWriter.append(datum);
        	}
        dataFileWriter.close();
    }
    
    public static void main(String[] args) throws Exception{
        new TextToAvro().run(args);
    }
}
