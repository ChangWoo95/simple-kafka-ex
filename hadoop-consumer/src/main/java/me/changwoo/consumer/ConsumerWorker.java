package me.changwoo.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// runnable 인터페이스는 무엇일까?
public class ConsumerWorker implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>(); // key: 파티션 번호, value: 메세지 값
    private static Map<Integer, Long> currentFileOffest = new ConcurrentHashMap<>(); // 오프셋 값을 저장하기 위함
    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number) {
        logger.info("Generate ConsumerWorker");
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer<>(prop); // 설정대로 컨슈머 인스턴스 생성
        consumer.subscribe(Arrays.asList(topic)); // 해당 토픽을 구독

        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for(ConsumerRecord<String, String> record : records) {
                    addHdfsFileBuffer(record);
                }

                saveBufferToHdfsFile(consumer.assignment());
            }
        } catch(WakeupException e) {
            logger.warn("Wakeup consume");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    private void addHdfsFileBuffer(ConsumerRecord<String, String> record) { // 레코드를 받아 메세지 값을 버퍼에 저장
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>()); // 파티션번호에 해당하는 list가 존재하면 가져오고 아니면 new arrayList
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        if(buffer.size() == 1) {
            currentFileOffest.put(record.partition(), record.offset());
        }
    }

    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) { //컨슈머 스레드에 할당된 파티션에만 접근
        partitions.forEach(p -> checkFlushCount(p.partition()));
    }

    private void checkFlushCount(int partitionNo) { // 정한 갯수만큼 차있다면 flush 진행
        if(bufferString.get(partitionNo) != null) {
            if(bufferString.get(partitionNo).size() > FLUSH_RECORD_COUNT - 1) {
                save(partitionNo);
            }
        }
    }

    private void save(int partitionNo) {
        if(bufferString.get(partitionNo).size() > 0) {
            try {
                String fileName = "/data/color~" + partitionNo + "-" + currentFileOffest.get(partitionNo) + ".log";
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem hdfsFileSystem = FileSystem.get(configuration);
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n"));
                fileOutputStream.close();

                bufferString.put(partitionNo, new ArrayList<>());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private void saveRemainBufferToHdfsFile() {
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }

    public void stopAndWakeup() {
        logger.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }
}
