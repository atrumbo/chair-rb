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

    private static native boolean createAndStartProducer(ByteBuffer byteBuffer);

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
    }

    private static class OneToOneExample implements MessageHandler {
        final ByteBuffer byteBuffer  = ByteBuffer.allocateDirect(1024+768);
        final AtomicBuffer atomicBuffer = new UnsafeBuffer(byteBuffer);

        final RingBuffer ringBuffer = new OneToOneRingBuffer(atomicBuffer);

        int timesCalled = 0;
        boolean isPoision = false;

        public void run(){
            System.out.println("One To One RB - Rust Producer/Java Consumer ");
            final boolean started = createAndStartProducer(byteBuffer);
            System.out.println("Native Producer Started: " + started);

            var start = Instant.now();
            while (!isPoision){
                ringBuffer.read(this);
            }
            var finish = Instant.now();
            var elapsed = Duration.between(start, finish);
            System.out.printf(
            "Time to receive %d messages %d milliseconds %d message per second\n",
                    timesCalled,
                    elapsed.toMillis(),
                    timesCalled / elapsed.toSeconds());
        }

        @Override
        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
            timesCalled++;
            var value = buffer.getLong(index);
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

}