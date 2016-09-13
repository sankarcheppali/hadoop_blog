import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.xerces.impl.xs.opti.SchemaDOMParser;

public class SpecificAPIExample {

	public void run() throws IOException{
		StringPair sp=new StringPair("Hello","World");
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		DatumWriter<StringPair> writer=new SpecificDatumWriter<StringPair>(StringPair.class);
		Encoder encoder=EncoderFactory.get().binaryEncoder(out, null);
		writer.write(sp, encoder);
		encoder.flush();
		System.out.println("Serilization complete");
		
		DatumReader<StringPair> reader= new SpecificDatumReader<StringPair>(StringPair.class);
		Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		StringPair dsp=reader.read(null, decoder);
		System.out.println("Deserialization Complete");
		System.out.println(sp.toString());
		System.out.println(dsp.toString());
	}
	public static void main(String[] args) throws IOException{
		new SpecificAPIExample().run();
	}
}
