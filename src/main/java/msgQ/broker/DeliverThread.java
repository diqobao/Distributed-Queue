package msgQ.broker;

public class DeliverThread extends Thread {
    Broker broker;
    public DeliverThread(Broker broker) {
        this.broker = broker;
    }

}
