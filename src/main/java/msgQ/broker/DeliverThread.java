package msgQ.broker;

public class DeliverThread extends Thread {
    Broker broker;
    public DeliverThread(Broker broker) {
        this.broker = broker;
    }

    public void run() {
        try {
            while (true) {
                broker.sendNewRecords();
            }
        } catch (InterruptedException e) {

        }
    }
}
