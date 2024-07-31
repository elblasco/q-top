package it.unitn.disi.ds1.qtop;

import java.util.Scanner;

/**
 * UserInterface class to manage/generate the user interface.
 */
public class UserInterface implements SimulationCallback {
    private Controller controller;

    public UserInterface(Controller controller) {
        this.controller = controller;
    }

    /**
     * Start the user interface.
     */
    public void start() {

        //default values
        int numberOfNodes = 10;
        int numberOfClients = 3;
        int decisionTimeout = 2000;
        int voteTimeout = 1000;
        int writeTimeout = 2000;

        System.out.println("Qtop - DS Project 2023/2024 - Blascovich Alessio, Cereser Lorenzo \n");

        System.out.println("""
                This is a simulation of 2PC + token ring leader election for the\s
                distributed system course, the system is composed by a number of\s
                nodes that can be set by the user. Virtual crashes can be inserted
                at user discretion but only in one place at a time. Timeouts can\s
                also be set. All of the node logs will be displayed in the specific\s
                file.""");
        System.out.println("_________________________________________________________________");
        Scanner scanner = new Scanner(System.in);
        int input = 0;
        while (input != 1) {
            System.out.println("MAIN MENU - Please select an option:");
            System.out.println("_________________________________________________________________");
            System.out.println("1. Start the simulation");
            System.out.println("2. Set the number of nodes [current: "+numberOfNodes+"]");
            System.out.println("3. Set the decision timeout in ms, [current: "+decisionTimeout+"ms]");
            System.out.println("4. Set the vote timeout in ms, [current: "+voteTimeout+"ms]");
            System.out.println("5. Set the write timeout in ms, [current: " + writeTimeout + "ms]");
            System.out.println("6. Set the number of clients [current: " + numberOfClients + "]");
            System.out.println("_________________________________________________________________");

            input = scanner.nextInt();

            switch (input) {
                case 1:
                    controller.startSimulation(
                            numberOfNodes,
                            numberOfClients,
                            decisionTimeout,
                            voteTimeout,
                            writeTimeout
                    );
                    break;
                case 2:
                    System.out.println("Insert the number of nodes:");
                    numberOfNodes = scanner.nextInt();
                    break;
                case 3:
                    System.out.println("Insert the decision timeout in ms:");
                    decisionTimeout = scanner.nextInt();
                    break;
                case 4:
                    System.out.println("Insert the vote timeout in ms:");
                    voteTimeout = scanner.nextInt();
                    break;
                case 5:
                    System.out.println("Insert the write timeout in ms:");
                    writeTimeout = scanner.nextInt();
                    break;
                case 6:
                    System.out.println("Insert the number of clients:");
                    numberOfClients = scanner.nextInt();
                    break;
                default:
                    System.out.println("Invalid option, please try again");
                    break;
            }
        }

    }

    /**
     * Display the client menu.
     */
    @Override
    public void clientMenu() {
        System.out.println("_________________________________________________________________");
        Scanner scanner = new Scanner(System.in);
        int input = 0;
        while (input != 2) {
            System.out.println("CLIENT MENU - Please select an option:");
            System.out.println("_________________________________________________________________");
            System.out.println("1. Insert a virtual crash in a random node");
            System.out.println("2. Exit simulation");
            System.out.println("_________________________________________________________________");

            input = scanner.nextInt();

            switch (input) {
                case 1:
                    System.out.println("Select the position of the virtual crash:");
                    System.out.println("1. NODE - Before write request");
                    System.out.println("2. NODE - After write request");
                    System.out.println("3. NODE - After vote request received");
                    System.out.println("4. NODE - After vote casted");
                    System.out.println("5. COORDINATOR - Before R/W request");
                    System.out.println("6. COORDINATOR - After R/W request");
                    System.out.println("7. COORDINATOR - During votes reception, NO quorum reached");
                    System.out.println("8. COORDINATOR - During votes reception, quorum reached");
                    System.out.println("9. COORDINATOR - During the multicast of a message");

                    int crashType = scanner.nextInt();
                    controller.crashNode(crashType);
                    break;
                case 2:
                    System.out.println("Exiting simulation");
                    controller.exitSimulation();
                    break;
                default:
                    System.out.println("Invalid option, please try again");
                    break;
            }
        }
    }

    /**
     * Set the Controller.
     *
     * @param controller the controller
     */
    public void setController(Controller controller) {
        this.controller = controller;
    }
}
