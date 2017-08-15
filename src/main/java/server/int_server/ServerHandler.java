package server.int_server;
import java.util.Scanner;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.util.JsonFormat;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import kafka.Producer;
import server.proto.PersonOuterClass.Person;

/**
 * Handles a server-side channel.
 */

public class ServerHandler extends ChannelInboundHandlerAdapter { // (1)

	static String topic = null;

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws InvalidProtocolBufferException { // (2)
		String req_type = null ;
		String json = null;
		FullHttpRequest HttpRequest = null;
		try
		{

			HttpRequest = (FullHttpRequest) msg;
			req_type = 	HttpRequest.method().toString() ;
			System.out.println("Request Mode is " + req_type);
			Builder obj = Person.newBuilder();

			Person person;
			if(req_type.equals("GET") || req_type.equals("POST"))
			{


				if(req_type.equals("POST"))
				{

					byte[] x = new byte[HttpRequest.content().capacity()];

					HttpRequest.content().readBytes(x);

					json = new String(x);

					JsonFormat.parser().merge(json, obj );

					person = (Person) obj.build();

					System.out.println("Json converted to protobuf Successfully..");

				}

				else
				{
					QueryStringDecoder decoder = new QueryStringDecoder(HttpRequest.getUri());

					System.out.println("name is "+decoder.parameters().get("name").get(0));

					person = Person.newBuilder()
							.setName(decoder.parameters().get("name").get(0))
							.setId(Integer.parseInt(decoder.parameters().get("id").get(0)))
							.setEmail(decoder.parameters().get("email").get(0))
							.build();

					System.out.println("URL Request parameter converted to protobuf Successfully..");

				}


				System.out.println(person.toString());


				if(topic==null)
				{	Scanner s = new Scanner(System.in);
				System.out.print("Enter the Topic Name :");
				topic = s.next();
				s.close();
				}
				Producer.producer(topic,person);  
			}
		} finally {
			ReferenceCountUtil.release(msg); 
		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { 
		cause.printStackTrace();
		ctx.close();
	}
}
