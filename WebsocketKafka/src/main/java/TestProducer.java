
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import com.neovisionaries.ws.client.*;
import org.json.JSONObject;

public class TestProducer {

    private final static String TOPIC = "streaming.bitfinex.candles";
    private final static String BOOTSTRAP_SERVERS = "m1.adaltas.com:6667";
    public final static String BITFINEX_URI = "wss://api.bitfinex.com/ws/2";
    public static long messages_count = 0;
    public static Long buffer_mts;
    public static String[] buffer;

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducer(5);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }
    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        final long time = System.currentTimeMillis();

	String jsonString = new JSONObject()
                .put("event", "subscribe")
                .put("channel", "candles")
                .put("key","trade:1m:tBTCUSD").toString();

	WebSocket websocket = new WebSocketFactory()
                .createSocket(BITFINEX_URI)
                .addListener(new WebSocketAdapter() {
                    @Override
                    public void onTextMessage(WebSocket ws, String message) {
                        String chunks[];
                        message = message.replaceAll("[\\[\\]\"]", "");
                        chunks = message.split(",");
                        messages_count ++;
			
                        if(messages_count == 3){
                            buffer = chunks;
                            buffer_mts = Long.parseLong(chunks[1]);
                        }
                        else if(messages_count > 3){
                            if(buffer_mts == Long.parseLong(chunks[1])){
                                buffer = chunks;
                                buffer_mts = Long.parseLong(chunks[1]);
                            }
                            else if (buffer_mts < Long.parseLong(chunks[1])){
                                buffer = chunks;
                                buffer_mts = Long.parseLong(chunks[1]);
				try {
                            
	                            final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, messages_count, message);

        	                    RecordMetadata metadata = producer.send(record).get();

                	            long elapsedTime = System.currentTimeMillis() - time;
                        	    System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                                                  record.key(), record.value(),
                                                  metadata.partition(),
                                                  metadata.offset(), elapsedTime);
                             
                       	        }
                                catch(java.lang.InterruptedException ie){
                                    ie.printStackTrace();
                                }
                                catch(java.util.concurrent.ExecutionException ee){
                                    ee.printStackTrace();
                                }
                                finally {
				    if(System.currentTimeMillis() > time + 1170000){
                                        producer.flush();
                                        producer.close();
				    }
                                }

			    }
			}
		    }
		})
		.connect();

	websocket.sendText(jsonString);
        Thread.sleep(1200000);
        websocket.disconnect();

    }

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }


}

