import org.junit.Test;
import static org.junit.Assert.*;




public class MRTextToAvroTest  {
	@Test
	public void TestTagsExtractor(){
             String[] tags= new MRTextToAvro.TextToAvroMapper().getMovieTags("1::Toy Story (1995)::Animation|Children's|Comedy");
             System.out.println(tags.length);
             assertArrayEquals(tags,new String[]{"Animation","Children's","Comedy"});
	}
}
