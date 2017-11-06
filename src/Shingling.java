import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.List;

public class Shingling {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataset = env.readTextFile("/Users/Amir/IdeaProjects/Lab1DataMining/dataset-CalheirosMoroRita-2017.csv");

        //dataset.print();

        DataStream<Tuple2<Character,Character>> shingles = dataset.flatMap(new FlatMapFunction<String, Tuple2<Character, Character>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Character, Character>> collector) throws Exception {
               // System.out.println("S är " + s);
                char[] cArray = s.toCharArray();
                for (int i = 0; i<cArray.length; i++){
                    Tuple2<Character, Character> shingle = new Tuple2<>(cArray[i], cArray[i+1]);
                    i++;
                    collector.collect(shingle);
                }
            }
        });

        shingles.print();
        env.execute();

    }



}
