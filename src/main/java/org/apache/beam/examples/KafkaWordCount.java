/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

/**
 * An example that counts words from Kafka stream
 *
 * <pre>
 *   1. Reading data from a Kafka topic
 *   2. Specifying 'inline' transforms
 *   3. Assign a window
 *   4. Counting items in a PCollection
 *   5. Writing data to an output kakfa topic
 * </pre>
 *
 * <p>Create the input topic before running:
 * <pre>{@code
 * $ ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic input-text --partitions 10 --replication-factor 1
 * }</pre>
 *
 * <p>To run locally:
 * <pre>{@code
 * $ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.KafkaWordCount \
 * -Dexec.args="--runner=SamzaRunner" -P samza-runner
 * }</pre>
 *
 * <p>To execute the example in distributed manner, use mvn to package it first:
 * (remove .waitUntilFinish() in the code for yarn deployment)
 * <pre>{@code
 * $ mkdir -p deploy/examples
 * $ mvn package && tar -xvf target/samza-beam-examples-0.1-dist.tar.gz -C deploy/examples/
 * }</pre>
 *
 * <p>To run in standalone with zookeeper:
 * (large parallelism will enforce each partition in a task)
 * <pre>{@code
 * $ ./deploy/examples/bin/run-beam-standalone.sh org.apache.beam.examples.KafkaWordCount --configFilePath=$PWD/deploy/examples/config/standalone.properties --maxSourceParallelism=1024
 * }</pre>
 *
 * <p>To run in yarn:
 * For yarn, we don't need to wait after submitting the job, so there is no need for
 * waitUntilFinish(). Please change p.run().waitUtilFinish() to p.run().
 *
 * (large parallelism will enforce each partition in a task)
 * <pre>{@code
 * $ ./deploy/examples/bin/run-beam-yarn.sh org.apache.beam.examples.KafkaWordCount --configFilePath=$PWD/deploy/examples/config/yarn.properties --maxSourceParallelism=1024
 * }</pre>
 *
 * <p>To produce some test data:
 * <pre>{@code
 * $ ./deploy/kafka/bin/kafka-console-producer.sh --topic input-text --broker-list localhost:9092 <br/>
 * Nory was a Catholic because her mother was a Catholic, and Nory’s mother was a Catholic because her father was a Catholic, and her father was a Catholic because his mother was a Catholic, or had been. </br>
 * }</pre>
 *
 * <p>To verify output:
 * <pre>{@code
 * $ ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-count --property print.key=true
 * }</pre>
 *
 */
public class KafkaWordCount {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    options.setJobName("kafka-word-count");

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    p.apply(
        KafkaIO.<String, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("input-text")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withoutMetadata())
        .apply(Values.create())
        // Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
        // We use a Filter transform to avoid empty word
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        // Apply the Count transform to our PCollection of individual words. The Count
        // transform returns a new PCollection of key/value pairs, where each key represents a
        // unique word in the text. The associated value is the occurrence count for that word.
        .apply(Count.perElement())
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(kv -> KV.of(kv.getKey(), String.valueOf(kv.getValue()))))
        .apply(KafkaIO.<String, String>write()
            .withBootstrapServers("localhost:9092")
            .withTopic("word-count")
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class));

    // For yarn, we don't need to wait after submitting the job,
    // so there is no need for waitUntilFinish(). Please use
    // p.run()
    p.run().waitUntilFinish();
  }
}
