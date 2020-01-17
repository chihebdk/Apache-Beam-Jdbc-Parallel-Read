import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class Test {

	public static void main(String[] args) {
		

		
		ObjectMapper mapper = new ObjectMapper();
		
		
		
		try {
			ObjectNode node = mapper.createObjectNode();
			node.put("1", "o");
			node.put("2", "b");
			
			 String s = mapper.writeValueAsString(node);
			 
			 Map<String, String> map = mapper.readValue(s, new TypeReference<Map<String,String>>(){});
			
			 

			 System.out.println("o");
		
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
