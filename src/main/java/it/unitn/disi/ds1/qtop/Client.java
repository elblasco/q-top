package it.unitn.disi.ds1.qtop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.disi.ds1.qtop.Utils.ReadRequest;
import it.unitn.disi.ds1.qtop.Utils.WriteRequest;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static it.unitn.disi.ds1.qtop.Utils.LogLevel;
import static it.unitn.disi.ds1.qtop.Utils.StartMessage;

public class Client extends AbstractActor{
	private static final int CRASH_TIMEOUT = 1000;
	private static final Random rand = new Random();
	private static final Logger logger = Logger.getInstance();
	private final int clientId;
	private List<ActorRef> group;
	private int numberOfNodes;
	private int timeOutCounter;
	private Cancellable crashTimeOut;
	private Utils.CrashType cachedCrashType;

    public Client(int clientId, List<ActorRef> group, int numberOfNodes) {
        super();
        this.clientId = clientId;
		this.group = group;
		this.numberOfNodes = numberOfNodes;
	    this.timeOutCounter = CRASH_TIMEOUT;
    }

    static public Props props(int clientId, List<ActorRef> group, int numberOfNodes) {
        return Props.create(Client.class, () -> new Client(clientId, group, numberOfNodes));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(
                StartMessage.class,
                this::onStartMessage
        ).match(
				Utils.MakeRequest.class,
		        this::onMakeRequest
        ).match(
				Utils.ReadValue.class,
		        this::onReadValue
        ).match(
		        Utils.CrashRequest.class,
		        this::onCrashRequest
        ).match(
		        Utils.TimeOut.class,
		        this::onTimeOut
        ).match(
		        Utils.CrashACK.class,
		        this::onCrashACK
        ).match(
		        Utils.CountDown.class,
		        this::onCountDown
        ).matchAny(tmp -> {
        }).build();
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
			    new Utils.MakeRequest(
					    r.nextBoolean(),
					    r.nextInt(this.numberOfNodes)
			    ),
			    getContext().getSystem().dispatcher(),
			    this.getSelf()
	    );

    }

	private void onTimeOut(Utils.TimeOut msg) {
		if (msg.reason() == Utils.TimeOutReason.CRASH_RESPONSE)
		{
			this.crashTimeOut.cancel();
			logger.log(
					LogLevel.INFO,
					"[CLIENT-" + this.clientId + "] crash response not received, I am going to resend it"
			);
			this.timeOutCounter = CRASH_TIMEOUT;
			// make another crash request to another random `Node`
			getSelf().tell(
					new Utils.CrashRequest(this.cachedCrashType),
					getSelf()
			);
		}
	}

	private void onMakeRequest(Utils.MakeRequest msg){
		int index = msg.indexTarget();
		if(msg.kindOfRequest())
		{
			//WRITE
			// the new values are from 0 to 100
			int proposedValue = new Random().nextInt(101);
			group.get(index).tell(
					new WriteRequest(
							proposedValue,
							- 1
					),
					this.getSelf()
			);
			logger.log(
					LogLevel.INFO,
					"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] write req to [NODE-" + index + "] of value "
							+ proposedValue
			);
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
		logger.log(
				LogLevel.INFO,
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] read done from node " + getSender() + " of " +
						"value " + value
		);
	}

	private void onCountDown(Utils.CountDown msg) {
		if (msg.reason() == Utils.TimeOutReason.CRASH_RESPONSE)
		{
			if (this.timeOutCounter <= 0)
			{
				getSelf().tell(
						new Utils.TimeOut(Utils.TimeOutReason.CRASH_RESPONSE),
						getSelf()
				);
			}
			else
			{
				this.timeOutCounter -= CRASH_TIMEOUT / 100;
			}
		}
	}

	private void onCrashRequest(Utils.CrashRequest msg) {
		this.cachedCrashType = msg.crashType();
		this.crashTimeOut = this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
				Duration.ZERO,
				Duration.ofMillis(100),
				getSelf(),
				new Utils.CountDown(
						Utils.TimeOutReason.CRASH_RESPONSE,
						null
				),
				getContext().getSystem().dispatcher(),
				getSelf()
		);
		this.group.get(rand.nextInt(this.numberOfNodes)).tell(
				msg,
				getSelf()
		);
	}

	private void onCrashACK(Utils.CrashACK msg) {
		this.crashTimeOut.cancel();
		this.timeOutCounter = CRASH_TIMEOUT;
		logger.log(
				LogLevel.INFO,
				"[CLIENT-" + this.clientId + "] received crash ack from " + getSender()
		);
	}
}
