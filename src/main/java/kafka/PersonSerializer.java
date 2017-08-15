package kafka;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import server.proto.PersonOuterClass.Person;

public class PersonSerializer implements Serializer<Person> {
	private String encoding = "UTF8";

	public void close() {
		// TODO Auto-generated method stub

	}

	public void configure(Map<String, ?> arg0, boolean data) {
		// TODO Auto-generated method stub

	}

	public byte[] serialize(String arg0, Person data) {

		int sizeOfName;
		int sizeOfId;
		int sizeOfEmail;
		byte[] serializedName;
		byte[] serializedId;
		byte[] serializedEmail;

		try {
			if (data == null)
				return null;
	
			serializedName = data.getName().getBytes(encoding);
			sizeOfName = serializedName.length;
			
			
			serializedId = Integer.toString(data.getId()).getBytes(encoding);
			sizeOfId = serializedId.length;
			
			serializedEmail = data.getEmail().getBytes(encoding);
			sizeOfEmail = serializedEmail.length;

			ByteBuffer buf = ByteBuffer.allocate(4+sizeOfName+4+sizeOfId+4+sizeOfEmail);
			
			buf.putInt(sizeOfName);
			buf.put(serializedName);
			
			buf.putInt(sizeOfId);
			buf.put(serializedId);
			
			buf.putInt(sizeOfEmail);
			buf.put(serializedEmail);

			System.out.println("ProtoBuf Object successfully converted to Byte of streams...");
			
			return buf.array();
			
			

		} catch (Exception e) {
			System.out.println("Exception");
			System.out.println(e.getStackTrace());
			throw new SerializationException("Error when serializing Supplier to byte[]");
			
		}

		
	}


}