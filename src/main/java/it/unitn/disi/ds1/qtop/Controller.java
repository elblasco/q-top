package it.unitn.disi.ds1.qtop;

public class Controller {
    private Simulation simulation;
    private UserInterface ui;
    private final Logger logger = Logger.getInstance();

    public Controller(Simulation simulation, UserInterface ui) {
        this.simulation = simulation;
        this.ui = ui;
    }

	public void startSimulation(int numberOfNodes, int numberOfClients, int decisionTimeout, int voteTimeout,
			int writeTimeout) {
		simulation.start(
				numberOfNodes,
				numberOfClients,
				decisionTimeout,
				voteTimeout,
				writeTimeout
		);
        clientMenu();
    }

	public int readVariable() {
		return simulation.readVariable();
    }

	public void writeVariable(int value) {
		simulation.writeVariable(value);
    }

	public void crashNode(int crashType) {
            logger.log(
                    Utils.LogLevel.INFO,
		            "[SYSTEM] inserting crash type " + crashType
            );

		this.simulation.addCrashNode(crashType);
    }

    public void exitSimulation() {
        simulation.exit();
    }

    public void clientMenu() {
        ui.clientMenu();
    }

    /*private boolean checkIndex(int nodeIndex) {
        return nodeIndex >= 0 && nodeIndex < this.simulation.getNumberOfNodes() && ! this.simulation.isNodeCrashed
        (nodeIndex);
    }*/
}
