package it.unitn.disi.ds1.qtop;

public class Controller {
    private Simulation simulation;
    private UserInterface ui;

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

    public void crashNode(int crashType) {
        //TODO: implement crashNode
    }


    public void exitSimulation() {
        simulation.exit();
    }

    public void clientMenu() {
        ui.clientMenu();
    }


}
