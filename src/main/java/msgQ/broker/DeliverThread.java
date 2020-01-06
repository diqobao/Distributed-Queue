package msgQ.broker;

public class DeliverThread extends Thread {
    Broker broker;
    public DeliverThread(Broker broker) {
        this.broker = broker;
    }

    public void run() {
        try {
            while (broker.isPrimary()) {
                broker.sendNewRecords();
            }
        } catch (InterruptedException e) {

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}