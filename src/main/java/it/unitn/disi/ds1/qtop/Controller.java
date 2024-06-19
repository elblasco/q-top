package it.unitn.disi.ds1.qtop;

public class Controller {
    private Simulation simulation;
    private UserInterface ui;
    private final Logger logger = Logger.getInstance();

    public Controller(Simulation simulation, UserInterface ui) {
        this.simulation = simulation;
        this.ui = ui;
    }

    public void startSimulation(int numberOfNodes, int decisionTimeout, int voteTimeout) {
        simulation.start(numberOfNodes, decisionTimeout, voteTimeout);
        clientMenu();
    }

    public int readVariable(int node){
        simulation.readVariable(node);
        return 0;
    }

    public void writeVariable(int node, int value) {
        simulation.writeVariable(node, value);
    }

	/**
	 * Function to make crash a Node with a specific index.
	 * @param crashIndex  Index of the node that has to crash.
	 *                    The special value of -1 is used when the
	 *                    coordinator has to be killed.
	 * @param crashType   Number corresponding to the crash type
	 */
    public void crashNode(int crashIndex, int crashType) {
		if (crashIndex == -1)
		{
            logger.log(
                    Utils.LogLevel.INFO,
                    "[SYSTEM] inserting crash type " + crashType + " into coordinator"
            );
		}
        else if (checkIndex(crashIndex))
        {
            logger.log(
                    Utils.LogLevel.INFO,
                    "[SYSTEM] inserting crash type " + crashType + " into node: " + crashIndex
            );
        }
        else
        {
            logger.log(
                    Utils.LogLevel.INFO,
                    "[SYSTEM] node " + crashIndex + " can not crash"
            );
        }
		this.simulation.addCrashNode(
									 crashIndex,
									 crashType
									 );
    }

    public void exitSimulation() {
        simulation.exit();
    }

    public void clientMenu() {
        ui.clientMenu();
    }

    private boolean checkIndex(int nodeIndex) {
        return nodeIndex >= 0 && nodeIndex < this.simulation.getNumberOfNodes() && ! this.simulation.isNodeCrashed(nodeIndex);
    }
}
