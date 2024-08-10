package it.unitn.disi.ds1.qtop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static it.unitn.disi.ds1.qtop.Utils.*;

/**
 * The Client actor is responsible for sending requests to the Nodes in the network. The requests are randomised
 * between read and write operations. The Client actor also has the ability to make crash a random Node in the network.
 */
public class Client extends AbstractActor {
	private static final Random RAND = new Random();
	private static final Logger LOGGER = Logger.getInstance();
	private static final int CRASH_TIMEOUT = 1000;
	private static final int CLIENT_REQUEST_TIMEOUT = 1000;
	private static final int COUNTDOWN_REFRESH = 10;
	private final TimeOutManager timeOutManager;
	private final List<ActorRef> group;
	private final int clientId;
	private final int numberOfNodes;
	private Cancellable crashTimeOut;
	private Utils.CrashType cachedCrashType;
	private int timeOutCounter;
	private int requestNumber;

	/**
	 * Constructor for the Client actor.
	 *
	 * @param clientId      the client id
	 * @param group         the list of Nodes in the network
	 * @param numberOfNodes the number of Nodes in the network
	 */
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
				0,
				CLIENT_REQUEST_TIMEOUT,
				COUNTDOWN_REFRESH
		);
	}

	/**
	 * Wrapper for the Client actor.
	 *
	 * @param clientId      the client id
	 * @param group         the list of Nodes in the network
	 * @param numberOfNodes the number of Nodes in the network
	 *
	 * @return the Props object
	 */
	static public Props props(int clientId, List<ActorRef> group, int numberOfNodes) {
		return Props.create(
				Client.class,
				() -> new Client(
						clientId,
						group,
						numberOfNodes
				)
		);
	}

	/**
	 * Mask for to the Client actor.
	 *
	 * @return the Receive object
	 */
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
	 * Initial set up for the Client. It sets up a scheduled message to send requests to the `Node`s.
	 *
	 * @param msg the init message
	 */
	public void onStartMessage(StartMessage msg) {
		LOGGER.log(
				LogLevel.INFO,
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] starting..."
		);
		this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
				Duration.ZERO,
				Duration.ofMillis(1000),
				this.getSelf(),
				new Utils.MakeRequest(),
				getContext().getSystem().dispatcher(),
				this.getSelf()
		);
	}

	/**
	 * General purpose handler for the Client's timeout messages.
	 *
	 * @param msg Generic `TimeOut` message
	 */
	private void onTimeOut(@NotNull Utils.TimeOut msg) {
		if (msg.reason() == Utils.TimeOutReason.CRASH_RESPONSE)
		{
			this.crashTimeOut.cancel();
			LOGGER.log(
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
		else if (msg.reason() == Utils.TimeOutReason.CLIENT_REQUEST)
		{
			LOGGER.log(
					LogLevel.INFO,
					"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] request number " + msg.epoch()
							.i() + " timed" + " out "
			);
		}
	}

	/**
	 * Make request to a Node within the network, the request is randomised among either read or write.
	 *
	 * @param msg the scheduled message to make a request
	 */
	private void onMakeRequest(Utils.MakeRequest msg) {
		boolean type = RAND.nextBoolean(); // true for write, false for read
		int index = RAND.nextInt(this.numberOfNodes);
		this.timeOutManager.startCountDown(
				Utils.TimeOutReason.CLIENT_REQUEST,
				this.getContext().getSystem().scheduler().scheduleWithFixedDelay(
						Duration.ZERO,
						Duration.ofMillis(CLIENT_REQUEST_TIMEOUT / COUNTDOWN_REFRESH),
						this.getSelf(),
						new Utils.CountDown(
								Utils.TimeOutReason.CLIENT_REQUEST,
								new EpochPair(
										0,
										requestNumber
								)
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
			int proposedValue = RAND.nextInt(101);
			group.get(index).tell(
					new WriteRequest(
							proposedValue,
							requestNumber
					),
					this.getSelf()
			);
			LOGGER.log(
					LogLevel.INFO,
					"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] write requested to [NODE-" + index + "], " + "new value proposed: " + proposedValue + ", local operation id: " + requestNumber
			);
		}
		else
		{
			LOGGER.log(
					LogLevel.INFO,
					"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] requesting read to " + "[NODE-" + index +
							"]" + ", local operation id: " + requestNumber
			);
			group.get(index).tell(
					new ReadRequest(requestNumber),
					this.getSelf()
			);
		}
		this.requestNumber++;
	}

	/**
	 * Handler for the read request response from the Node.
	 *
	 * @param msg the read response message
	 */
	private void onReadAck(@NotNull Utils.ReadValue msg) {
		int value = msg.value();
		this.timeOutManager.resetCountDown(
				Utils.TimeOutReason.CLIENT_REQUEST,
				msg.nRequest()
		);
		LOGGER.log(
				LogLevel.INFO,
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] read done from " + Utils.matchNodeID(this.getSender()) + " of " + "value " + value + ", local operation id: " + msg.nRequest()
		);
	}

	/**
	 * Handler for the write request response from the Node.
	 *
	 * @param msg the write response message
	 */
	private void onWriteAck(@NotNull Utils.WriteValue msg) {
		this.timeOutManager.resetCountDown(
				Utils.TimeOutReason.CLIENT_REQUEST,
				msg.nRequest()
		);
		LOGGER.log(
				LogLevel.INFO,
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] write request done, local operation id: " + msg.nRequest()
		);

	}

	/**
	 * General purpose handler for the countdown message.
	 *
	 * @param msg the countdown message
	 */
	private void onCountDown(@NotNull Utils.CountDown msg) {
		if (msg.reason() == Utils.TimeOutReason.CRASH_RESPONSE)
		{
			if (this.timeOutCounter <= 0)
			{
				getSelf().tell(
						new Utils.TimeOut(
								Utils.TimeOutReason.CRASH_RESPONSE,
								null
						),
						getSelf()
				);
			}
			else
			{
				this.timeOutCounter -= CRASH_TIMEOUT / 100;
			}
		}
		else if (msg.reason() == Utils.TimeOutReason.CLIENT_REQUEST)
		{
			this.timeOutManager.clientHandleCountDown(
					msg.reason(),
					msg.epoch().i(),
					this
			);

		}
	}

	/**
	 * General purpose handler for the crash request message. Forward a crash request to a random Node in the
	 * network.
	 *
	 * @param msg the crash request message
	 */
	private void onCrashRequest(@NotNull Utils.CrashRequest msg) {
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
		this.group.get(RAND.nextInt(this.numberOfNodes)).tell(
				msg,
				getSelf()
		);
	}

	/**
	 * Handler for the crash request response from the Node.
	 *
	 * @param msg the crash response message
	 */
	private void onCrashACK(Utils.CrashACK msg) {
		this.crashTimeOut.cancel();
		this.timeOutCounter = CRASH_TIMEOUT;
		LOGGER.log(
				LogLevel.INFO,
				"[CLIENT-" + (this.clientId - this.numberOfNodes) + "] received crash insertion message ack from " + getSender()
		);
	}

	/**
	 * Send a message to a destination actor with a random delay, within 0 and 29 milliseconds.
	 *
	 * @param dest   the destination actor
	 * @param msg    the message to send
	 * @param sender the sender actor
	 */
	public void tell(ActorRef dest, final Object msg, final ActorRef sender) {
		this.getContext().getSystem().scheduler().scheduleOnce(
				Duration.ofMillis(RAND.nextInt(30)),
				dest,
				msg,
				this.getContext().getSystem().dispatcher(),
				sender
		);
	}
}
