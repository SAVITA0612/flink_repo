package com.flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.common.RandomNumberGeneration;

public class FlatMap_Transformation {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setBufferTimeout(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(1);

		DataStream<Tuple2<Integer, Integer>> inputDataStream = env.addSource(new RandomNumberGeneration());
		/**
		 * Takes one element and produces zero, one, or more elements. 
		 */
		inputDataStream.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Integer>() {
			public void flatMap(Tuple2<Integer, Integer> arg0, Collector<Integer> arg1) throws Exception {
				for (int i = 1; i < 5; i++)
					arg1.collect(i);
			}
		}).print();

		env.execute("FlatMapFunction Example");
	}

}
