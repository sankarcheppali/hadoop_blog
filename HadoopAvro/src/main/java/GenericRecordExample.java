import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import java.io.*;
public class GenericRecordExample  {
    public void run() throws Exception{
        Schema.Parser parser = new Schema.Parser();
    Schema schema=parser.parse(getClass().getResourceAsStream("StringPair.avsc"));
    GenericRecord datum=new GenericData.Record(schema);
   
     datum.put("FirstName","Sankar");
     
     datum.put("LastName","Reddy");
     ByteArrayOutputStream out =new ByteArrayOutputStream();
     DatumWriter<GenericRecord> writer=new GenericDatumWriter<GenericRecord>(schema);
     Encoder encoder=EncoderFactory.get().binaryEncoder(out,null);
     writer.write(datum,encoder);
     System.out.println("Serilization Completed");
     encoder.flush();
     //out.close();
     DatumReader<GenericRecord> reader=new GenericDatumReader<GenericRecord>(schema);
     Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
     GenericRecord result=reader.read(null,decoder);
     System.out.println("Record First Name :"+result.get("FirstName"));
    }
    public static void main (String[] args) throws Exception{
    new GenericRecordExample().run();
}

}
