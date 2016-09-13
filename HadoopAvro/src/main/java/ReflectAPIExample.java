
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.*;

public class ReflectAPIExample {
	public void run() throws IOException{
		
		Schema schema=ReflectData.get().getSchema(Person.class);
		DatumWriter<Person> writer=new ReflectDatumWriter<Person>(schema);
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		Encoder encoder=EncoderFactory.get().binaryEncoder(out, null);
		Person p=new Person("Sankar","Reddy");
		writer.write(p,encoder);
		encoder.flush();
		System.out.println("Serilization complete");
		
		DatumReader<Person> reader=new ReflectDatumReader<Person>(schema);
		Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		Person dep=reader.read(null, decoder);
		System.out.println("Deserilzation complete");
		System.out.println("Before serlization "+p);
		System.out.println("After deserilization "+p);
		
	}
	 public static void main (String[] args) throws Exception{
		    new ReflectAPIExample().run();
		}
}
