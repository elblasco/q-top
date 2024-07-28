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

import static it.unitn.disi.ds1.qtop.Utils.*;

public class Client extends AbstractActor{
	private static final int CRASH_TIMEOUT = 1000;
	private static final Random rand = new Random();
	private static final Logger logger = Logger.getInstance();
	public final int clientId;
	private List<ActorRef> group;
	private int numberOfNodes;
	private int timeOutCounter;
	private Cancellable crashTimeOut;
	private Utils.CrashType cachedCrashType;
	private TimeOutManager timeOutManager;

	private int requestNumber;

	private  static final int CLIENT_REQUEST_TIMEOUT = 1000;
	private static final int COUNTDOWN_REFRESH = 10;

    public Client(int clientId, List<ActorRef> group, int numberOfNodes) {
        super();
        this.clientId = clientId;
		this.group = group;
		this.numberOfNodes = numberOfNodes;
	    this.timeOutCounter = CRASH_TIMEOUT;
	    this.requestNumber = 0;
			    this.timeOutManager = new TimeOutManager(
			    0,
			    0,
			    0,
			    0,
				CLIENT_REQUEST_TIMEOUT,
			    COUNTDOWN_REFRESH
	    );



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
		        this::onReadAck
        ).match(
				Utils.WriteValue.class,
		        this::onWriteAck
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
        logger.log(LogLevel.INFO,"[CLIENT-"+ (this.clientId - this.numberOfNodes) +"] starting...");
	    this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
			    Duration.ZERO,
			    Duration.ofMillis(1000),
			    this.getSelf(),
			    new Utils.MakeRequest(),
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
		} else if (msg.reason() == Utils.TimeOutReason.CLIENT_REQUEST)
		{
			logger.log(
					LogLevel.INFO,
					"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] request number "+ msg.epoch().i()+" timed out "
			);
		}
	}

	private void onMakeRequest(Utils.MakeRequest msg){
		boolean type = rand.nextBoolean();
		int index = rand.nextInt(this.numberOfNodes);

		this.timeOutManager.startCountDown(
				Utils.TimeOutReason.CLIENT_REQUEST,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(CLIENT_REQUEST_TIMEOUT / COUNTDOWN_REFRESH),
						this.getSelf(),
						new Utils.CountDown(
								Utils.TimeOutReason.CLIENT_REQUEST,
								new EpochPair(0, requestNumber)
						),
						getContext().getSystem().dispatcher(),
						getSelf()
				),
				this.requestNumber

		);
		if (type)
		{
			//WRITE
			// the new values are from 0 to 100
			int proposedValue = rand.nextInt(101);
			group.get(index).tell(
					new WriteRequest(
							proposedValue,
							requestNumber
					),
					this.getSelf()
			);
			logger.log(
					LogLevel.INFO,
					"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] write requested to [NODE-" + index + "], " +
							"new value proposed: "
							+ proposedValue +
							", operation id: " + requestNumber
			);
		}
		else
		{

			logger.log(LogLevel.INFO,
					"[CLIENT-"+ (this.clientId - this.numberOfNodes) +"] read number " + requestNumber + " req to " +
							"[NODE-" + index + "]" + ", operation id: " + requestNumber);
			group.get(index).tell(new ReadRequest(requestNumber), this.getSelf());
			this.requestNumber++;
		}




	}

	private void onReadAck(Utils.ReadValue msg) {
		int value = msg.value();

		this.timeOutManager.resetCountDown(
				Utils.TimeOutReason.CLIENT_REQUEST,
				msg.nRequest(),
				this.clientId,
				logger
		);

		logger.log(
				LogLevel.INFO,
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] read done from node " + getSender() + " of " +
						"value" + value + ", operation id: " + msg.nRequest()
		);
	}

	private void onWriteAck(Utils.WriteValue msg){
		this.timeOutManager.resetCountDown(
				Utils.TimeOutReason.CLIENT_REQUEST,
				msg.nRequest(),
				this.clientId,
				logger
		);
		logger.log(
				LogLevel.INFO,
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] write request done, operation id: " + msg.nRequest()
		);

	}




	private void onCountDown(Utils.CountDown msg) {
		if (msg.reason() == Utils.TimeOutReason.CRASH_RESPONSE)
		{
			if (this.timeOutCounter <= 0)
			{
				getSelf().tell(
						new Utils.TimeOut(Utils.TimeOutReason.CRASH_RESPONSE,null),
						getSelf()
				);
			}
			else
			{
				this.timeOutCounter -= CRASH_TIMEOUT / 100;
			}
		} else if (msg.reason() == Utils.TimeOutReason.CLIENT_REQUEST) {
			this.timeOutManager.handleCountDown(
					msg.reason(),
					msg.epoch().i(),
					this,
					logger
			);

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
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] received crash insertion message ack from " + getSender()
		);
	}

	public void tell(ActorRef dest, final Object msg, final ActorRef sender) {
		try
		{
			Thread.sleep(rand.nextInt(10));
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		dest.tell(
				msg,
				sender
		);
	}
}
