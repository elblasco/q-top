package it.unitn.disi.ds1.qtop;

import java.util.Scanner;

public class UserInterface implements SimulationCallback {
    private Controller controller;

    public UserInterface(Controller controller) {
        this.controller = controller;
    }

    public void start() {

        //default values
        int numberOfNodes = 5;
        int decisionTimeout = 2000;
        int voteTimeout = 1000;
        int crashPoint = 0;

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
            System.out.println("_________________________________________________________________");

            input = scanner.nextInt();

            switch (input) {
                case 1:
                    controller.startSimulation(numberOfNodes, decisionTimeout, voteTimeout);
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
                default:
                    System.out.println("Invalid option, please try again");
                    break;
            }
        }

    }

    @Override
    public void clientMenu() {
        System.out.println("_________________________________________________________________");
        Scanner scanner = new Scanner(System.in);
        int input = 0;
        while (input != 4) {
            System.out.println("CLIENT MENU - Please select an option:");
            System.out.println("_________________________________________________________________");
            System.out.println("1. Read the variable through a random node");
            System.out.println("2. Write the variable through a random node");
            System.out.println("3. Insert a virtual crash in a random node");
            System.out.println("4. Exit simulation");
            System.out.println("_________________________________________________________________");

            input = scanner.nextInt();

            switch (input) {
                case 1:
                    System.out.println("Read the variable");
                    controller.readVariable();
                    break;
                case 2:
                    System.out.println("Write the variable");
                    controller.writeVariable();
                    break;
                case 3:
                    System.out.println("Crash a node");
                    controller.crashNode();
                    break;
                case 4:
                    System.out.println("Exiting simulation");
                    controller.exitSimulation();
                    break;
                default:
                    System.out.println("Invalid option, please try again");
                    break;
            }
        }
    }
    public void setController(Controller controller) {
        this.controller = controller;
    }
}
