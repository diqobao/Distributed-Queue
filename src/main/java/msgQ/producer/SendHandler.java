package msgQ.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class SendHandler extends Thread {
    private ProducerRecord<?> record;
    private CountDownLatch countLatch;

    public SendHandler(ProducerRecord<?> record, CountDownLatch countLatch) {
        this.record = record;
        this.countLatch = countLatch;
    }

    @Override
    public void run() {
        int groupId = record.getGroupId();
//        ByteArrayOutputStream bao = new ByteArrayOutputStream();
//        ObjectOutputStream output = new ObjectOutputStream(bao);
//        try {
//            output.writeObject(record);
//            ByteBuffer bytes = ByteBuffer.wrap(bao.toByteArray());
//            PrimaryClients[groupId].sned(bytes);
//        } catch (IOException ioe) {
//            ioe.printStackTrace();
//        }
    }
}
