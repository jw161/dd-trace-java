package datadog.trace;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

public class MsgPackStatefulDeserializer {

  private ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

  public List<List<TreeMap<String, Object>>> deserialize(byte[] bytes) {
    try {
      return mapper.readValue(bytes, new TypeReference<List<List<TreeMap<String, Object>>>>() {});
    } catch (JsonParseException e) {
      throw new RuntimeException(e);   //todo: improve error handling
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);   //todo: improve error handling
    } catch (IOException e) {
      throw new RuntimeException(e);   //todo: improve error handling
    }
  }
}
