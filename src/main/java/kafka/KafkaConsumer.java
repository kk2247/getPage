package kafka;

/**
 * @author KGZ
 * @date 2019/1/2 20:38
 */
public class KafkaConsumer {
    public static void main(String[] args) {
        new PageConsumer("test-consumer-group").start();
    }
}
