package it.unitn.disi.ds1.qtop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.qtop.Utils.ReadRequest;
import it.unitn.disi.ds1.qtop.Utils.WriteRequest;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static it.unitn.disi.ds1.qtop.Utils.LogLevel;
import static it.unitn.disi.ds1.qtop.Utils.StartMessage;

public class Client extends AbstractActor{
    private final Logger logger = Logger.getInstance();
	private final int clientId;
	private List<ActorRef> group;
	private int numberOfNodes;

    public Client(int clientId, List<ActorRef> group, int numberOfNodes) {
        super();
        this.clientId = clientId;
		this.group = group;
		this.numberOfNodes = numberOfNodes;
    }

    static public Props props(int clientId, List<ActorRef> group, int numberOfNodes) {
        return Props.create(Client.class, () -> new Client(clientId, group, numberOfNodes));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(
                StartMessage.class,
                this::onStartMessage
        ).match(Utils.ReadValue.class,
		        value ->
			        logger.log(LogLevel.INFO,"[CLIENT-"+ (this.clientId - this.numberOfNodes) +"] read done " + value)
		).match(
				Utils.MakeRequest.class,
		        this::onMakeRequest
        ).match(
				Utils.ReadValue.class,
		        this::onReadValue
        ).match(
				Utils.VoteRequest.class,
				        tmp -> {}
		        ).match(
				        Utils.DecisionResponse.class,
				        tmp -> {
				        }
		        )
		        .build();
    }

    /**
     * Initial set up for the Coordinator, should be called whenever an ActorRef becomes Coordinator.
     * @param msg the init message
     */
    public void onStartMessage(StartMessage msg) {
        logger.log(LogLevel.INFO,"[CLIENT-"+this.clientId+"] starting...");
		Random r = new Random();
	    getContext().getSystem().scheduler().scheduleAtFixedRate(
			    Duration.ZERO,
			    Duration.ofMillis(1000),
			    this.getSelf(),
			    new Utils.MakeRequest(r.nextBoolean(), r.nextInt(this.numberOfNodes)),
			    getContext().getSystem().dispatcher(),
			    this.getSelf()
	    );

    }

	private void onMakeRequest(Utils.MakeRequest msg){
		int index = msg.indexTarget();
		if(msg.kindOfRequest())
		{
			//WRITE
			// the new values are from 0 to 100
			int proposedValue = new Random().nextInt(101);
			group.get(index).tell(new WriteRequest(proposedValue), this.getSelf());
			logger.log(LogLevel.INFO,"[CLIENT-"+ (this.clientId - this.numberOfNodes) +"] write req to " + index +
					" of value "+ proposedValue);
		}
		else
		{
			//READ
			logger.log(LogLevel.INFO,"[CLIENT-"+ (this.clientId - this.numberOfNodes) +"] read req to " + index);
			group.get(index).tell(new ReadRequest(), this.getSelf());
		}
	}

	private void onReadValue(Utils.ReadValue msg) {
		int value = msg.value();
		logger.log(LogLevel.INFO,"[CLIENT-"+ (this.clientId - this.numberOfNodes) +"] read done " + value);
	}
}
