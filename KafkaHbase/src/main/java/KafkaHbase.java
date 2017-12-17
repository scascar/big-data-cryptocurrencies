
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Properties;
import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;


public class KafkaHbase {

    public static void main(String[] args) throws Exception {

		
		//Kafka consumers  config
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "m1.adaltas.com:6667");
		properties.put("zookeeper.connect", "m1.adlatas.com:2181");
		properties.put("key.deserializer", StringDeserializer.class.getName());
		properties.put("group.id", "salvablaze");
		properties.put("value.deserializer", StringDeserializer.class.getName());
		
		//Creates two consumers for the candles and indicators
		KafkaConsumer<String,String> candleConsumer = new KafkaConsumer<String,String>(properties);
		KafkaConsumer<String,String> indicatorConsumer = new KafkaConsumer<String,String>(properties);
		
		candleConsumer.subscribe(Arrays.asList("streaming.bitfinex.candles"));
		indicatorConsumer.subscribe(Arrays.asList("streaming.bitfinex.indicators"));
		

		//Hbase Config
		Configuration config = HBaseConfiguration.create();
       	config.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		Connection conn = ConnectionFactory.createConnection(config);

		//Tables to write on: candles and indicators
	 	Table candleTable = conn.getTable(TableName.valueOf("bitfinex.candles"));
		Table indicatorTable = conn.getTable(TableName.valueOf("bitfinex.indicators"));

		int candleRowkey = 0;
		int indicatorRowkey = 0;

		while(true){

			ConsumerRecords<String,String> candleRecords = candleConsumer.poll(100);
			for(ConsumerRecord<String,String> record: candleRecords){

				String[] values = record.value().split(",");
				System.out.println(record.value());
				Put valuePut = new Put(Bytes.toBytes(Integer.toString(candleRowkey)));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("channel_id"), Bytes.toBytes(values[0]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("timestamp"), Bytes.toBytes(values[1]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("open"), Bytes.toBytes(values[2]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("close"), Bytes.toBytes(values[3]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("high"), Bytes.toBytes(values[4]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("low"), Bytes.toBytes(values[5]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("volume"), Bytes.toBytes(values[6]));
				candleTable.put(valuePut);

				candleRowkey++;
			}

			ConsumerRecords<String,String> indicatorRecords = indicatorConsumer.poll(100);
			for(ConsumerRecord<String,String> record: indicatorRecords){

				String[] values = record.value().split(",");
				System.out.println(record.value());
				Put valuePut = new Put(Bytes.toBytes(Integer.toString(indicatorRowkey)));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("indicator"), Bytes.toBytes(values[0]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("timestamp"), Bytes.toBytes(values[1]));
				valuePut.add(Bytes.toBytes("values"),Bytes.toBytes("value"), Bytes.toBytes(values[2]));
				indicatorTable.put(valuePut);

				indicatorRowkey++;
			}

	}
	
    }
}
