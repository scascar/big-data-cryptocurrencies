import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
import org.apache.flink.util.Collector;

public class TestFlink {

    public static final String BITFINEX_INDICATORS_TOPIC = "streaming.bitfinex.indicators";
    public static final String BITFINEX_CANDLES_TOPIC = "streaming.bitfinex.candles";
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parse user parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010 < > (BITFINEX_CANDLES_TOPIC, new SimpleStringSchema(), parameterTool.getProperties()));

	DataStream<String> movingAverage7 = messageStream
		.flatMap(new FlatMapFunction<String, Tuple2<Long, Double>>() {
			@Override
			public void flatMap(String message, Collector<Tuple2<Long, Double>> out){
				int i = 0;
				long mts = 0;
				double ohlcAverage = 0;
				for(String value: message.split(",")){
					i ++;
					if(i%7 == 2 ){
						ohlcAverage = 0.0;
						mts = Long.parseLong(value);
					}
					else if(i%7 > 2){
						ohlcAverage += Double.parseDouble(value);
					}
				}
				ohlcAverage /= 4;
				out.collect(new Tuple2<Long, Double>(new Long(mts), new Double(ohlcAverage)));
			}
		})
		.windowAll(SlidingProcessingTimeWindows.of(Time.minutes(2), Time.minutes(1)))
		.trigger(CountTrigger.of(2))
		.<Tuple2<Long, Double>>apply(new AllWindowFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, TimeWindow>(){
			public void apply (TimeWindow window, Iterable<Tuple2<Long, Double>> tuples, Collector<Tuple2<Long, Double>> out){
				Double movingAverage = new Double(0.0);
				int nbValues = 0;
				Long mts = new Long(0);
				for(Tuple2 tuple: tuples){
					movingAverage = Double.sum(movingAverage, (Double)tuple.f1);
					nbValues ++;
					mts = (Long)tuple.f0;
				}
				movingAverage /= nbValues;
				out.collect(new Tuple2<Long, Double>(mts, movingAverage));
			}
		})
		.map(new MapFunction<Tuple2<Long, Double>, String>() {
			@Override
			public String map(Tuple2 tuple){
				return "MA7," + tuple.f0.toString() + "," + tuple.f1.toString();
			}
		});

	
	DataStream<String> movingAverage25 = messageStream
                .flatMap(new FlatMapFunction<String, Tuple2<Long, Double>>() {
                        @Override
                        public void flatMap(String message, Collector<Tuple2<Long, Double>> out){
                                int i = 0;
                                long mts = 0;
                                double ohlcAverage = 0;
                                for(String value: message.split(",")){
                                        i ++;
                                        if(i%7 == 2 ){
                                                ohlcAverage = 0.0;
                                                mts = Long.parseLong(value);
                                        }
                                        else if(i%7 > 2){
                                                ohlcAverage += Double.parseDouble(value);
                                        }
                                }
                                ohlcAverage /= 4;
                                out.collect(new Tuple2<Long, Double>(new Long(mts), new Double(ohlcAverage)));
                        }
                })
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.minutes(1)))
                .trigger(CountTrigger.of(3))
                .<Tuple2<Long, Double>>apply(new AllWindowFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, TimeWindow>(){
                        public void apply (TimeWindow window, Iterable<Tuple2<Long, Double>> tuples, Collector<Tuple2<Long, Double>> out){
                                Double movingAverage = new Double(0.0);
                                int nbValues = 0;
                                Long mts = new Long(0);
                                for(Tuple2 tuple: tuples){
                                        movingAverage = Double.sum(movingAverage, (Double)tuple.f1);
                                        nbValues ++;
                                        mts = (Long)tuple.f0;
                                }
                                movingAverage /= nbValues;
                                out.collect(new Tuple2<Long, Double>(mts, movingAverage));
                        }
                })
                .map(new MapFunction<Tuple2<Long, Double>, String>() {
                        @Override
                        public String map(Tuple2 tuple){
                                return "MA3," + tuple.f0.toString() + "," + tuple.f1.toString();
                        }
                });


	DataStream<String> movingAverage40 = messageStream
                .flatMap(new FlatMapFunction<String, Tuple2<Long, Double>>() {
                        @Override
                        public void flatMap(String message, Collector<Tuple2<Long, Double>> out){
                                int i = 0;
                                long mts = 0;
                                double ohlcAverage = 0;
                                for(String value: message.split(",")){
                                        i ++;
                                        if(i%7 == 2 ){
                                                ohlcAverage = 0.0;
                                                mts = Long.parseLong(value);
                                        }
                                        else if(i%7 > 2){
                                                ohlcAverage += Double.parseDouble(value);
                                        }

                                }
                                ohlcAverage /= 4;
                                out.collect(new Tuple2<Long, Double>(new Long(mts), new Double(ohlcAverage)));
                        }
                })
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .trigger(CountTrigger.of(5))
                .<Tuple2<Long, Double>>apply(new AllWindowFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, TimeWindow>(){
                        public void apply (TimeWindow window, Iterable<Tuple2<Long, Double>> tuples, Collector<Tuple2<Long, Double>> out){
                                Double movingAverage = new Double(0.0);
                                int nbValues = 0;
                                Long mts = new Long(0);
                                for(Tuple2 tuple: tuples){
                                        movingAverage = Double.sum(movingAverage, (Double)tuple.f1);
                                        nbValues ++;
                                        mts = (Long)tuple.f0;
                                }
                                movingAverage /= nbValues;
                                out.collect(new Tuple2<Long, Double>(mts, movingAverage));
                        }
                })
                .map(new MapFunction<Tuple2<Long, Double>, String>() {
                        @Override
                        public String map(Tuple2 tuple){
                                return "MA5," + tuple.f0.toString() + "," + tuple.f1.toString();
                        }
                });
	
        Properties properties = new Properties();
	properties.setProperty("bootstrap.servers", "m1.adaltas.com:6667");
	movingAverage7.addSink(new FlinkKafkaProducer010<>(BITFINEX_INDICATORS_TOPIC, new SimpleStringSchema(), properties));
	movingAverage25.addSink(new FlinkKafkaProducer010<>(BITFINEX_INDICATORS_TOPIC, new SimpleStringSchema(), properties));
	movingAverage40.addSink(new FlinkKafkaProducer010<>(BITFINEX_INDICATORS_TOPIC, new SimpleStringSchema(), properties));
	env.execute("BLLBLBLBLB");
    }
}
