import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;

class ChairRB {

    private static final int POISON_MESSAGE_TYPE = 42;

    private static final int MESSAGES_TO_PRODUCE = 1_000_000_000;

    private static native boolean createAndStartOneToOneProducer(ByteBuffer byteBuffer, int messageToProduce);

    static {
        try {
            System.loadLibrary("jnilib");
        } catch (Exception e) {
            System.err.println("Unable to load library" + e);
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        new OneToOneExample().run();
         new OneToOneJavaComparison().run();
    }

    private static class OneToOneExample implements MessageHandler {
        final ByteBuffer byteBuffer  = ByteBuffer.allocateDirect(1024+768);
        final AtomicBuffer atomicBuffer = new UnsafeBuffer(byteBuffer);

        final RingBuffer ringBuffer = new OneToOneRingBuffer(atomicBuffer);

        int timesCalled = 0;
        boolean isPoision = false;

        public void run(){
            System.out.println("One To One RB - Rust Producer/Java Consumer ");
            final boolean started = createAndStartOneToOneProducer(byteBuffer, MESSAGES_TO_PRODUCE);
            System.out.println("Rust Producer Started: " + started);

            final Instant start = Instant.now();
            while (!isPoision){
                ringBuffer.read(this);
            }
            final Instant finish = Instant.now();
            final Duration elapsed = Duration.between(start, finish);
            System.out.printf(
            "One To One RB - Rust Producer/Java Consumer - Time to receive %d messages %d milliseconds %d message per second\n",
                    timesCalled,
                    elapsed.toMillis(),
                    (long)timesCalled / elapsed.getSeconds());
        }

        @Override
        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
            timesCalled++;
            final long value = buffer.getLong(index);
            if (POISON_MESSAGE_TYPE == msgTypeId){
                isPoision = true;
                System.out.printf("Consumer - Got Poison - msg_type_id: %d value: %d, received total %d\n",
                        msgTypeId, value, timesCalled);
            }

            if (timesCalled % 100_000_000 == 0){
                System.out.println("Java Consumer - received " + timesCalled + " messages from rb");
            }
        }
    }

       private static class OneToOneJavaComparison implements MessageHandler {
            final ByteBuffer byteBuffer  = ByteBuffer.allocateDirect(1024+768);
            final AtomicBuffer atomicBuffer = new UnsafeBuffer(byteBuffer);

            final RingBuffer ringBuffer = new OneToOneRingBuffer(atomicBuffer);

            int timesCalled = 0;
            boolean isPoision = false;

            public void run(){
                System.out.println("One To One RB - Java Producer/Java Consumer ");
                final boolean started = startJavaProducer(ringBuffer);
                System.out.println("Java Producer Started: " + started);

                final Instant start = Instant.now();
                while (!isPoision){
                    ringBuffer.read(this);
                }
                final Instant finish = Instant.now();
                final Duration elapsed = Duration.between(start, finish);
                System.out.printf(
                "One To One RB - Java Producer/Java Consumer - Time to receive %d messages %d milliseconds %d message per second\n",
                        timesCalled,
                        elapsed.toMillis(),
                        (long)timesCalled / elapsed.getSeconds());
            }

            @Override
            public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
                timesCalled++;
                final long value = buffer.getLong(index);
                if (POISON_MESSAGE_TYPE == msgTypeId){
                    isPoision = true;
                    System.out.printf("Consumer - Got Poison - msg_type_id: %d value: %d, received total %d\n",
                            msgTypeId, value, timesCalled);
                }

                if (timesCalled % 100_000_000 == 0){
                    System.out.println("Java Consumer - received " + timesCalled + " messages from rb");
                }
            }

            private boolean startJavaProducer(RingBuffer ringBuffer){

                new Thread(() -> {
                    final AtomicBuffer srcBuffer = new UnsafeBuffer(new byte[128]);

                    int sent = 0;
                    while (sent < MESSAGES_TO_PRODUCE) {
                        srcBuffer.putLong(0, sent);

                        while (!ringBuffer.write(1, srcBuffer, 0, 8)){}
                        sent += 1;

                        if (sent % (MESSAGES_TO_PRODUCE / 10) == 0) {
                            System.out.printf("Producer - Written %d message to ring buffer\n", sent);
                        }

                    }
                    srcBuffer.putLong(0, POISON_MESSAGE_TYPE);
                    System.out.printf("Producer - Sending poison after publishing %d messages to ring buffer", sent);

                    while (!ringBuffer.write(POISON_MESSAGE_TYPE, srcBuffer, 0, 8)) {
                    }
                }).start();

                return true;
            }

        }


}