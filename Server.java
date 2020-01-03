package stackserver;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

// main class
public class Server {
  
  private final static int CONNECTION_LIMIT = 100;
  private final static int MAIN_PORT = 8080;
  private final static int STATUS_PORT = 8081;
  private static StackManager stackManager;
  private static ConnectionExecutor executor;
  private static ServerSocket inputSocket;
  private static ServerSocket statusSocket;

  public Server() {
    // Set up the stack instance
    stackManager = new StackManager();
    // Set up a thread pool to handle tcp connections
    executor = new ConnectionExecutor(CONNECTION_LIMIT);
    try {
      inputSocket = new ServerSocket(MAIN_PORT);
    } catch(Exception e){
      e.printStackTrace();
    }
    // Listen for push/pop requests on 8080
    // Listen for status requests on 8081
    setupStatusSocket(STATUS_PORT);
  }

  public void restart(){
    System.out.println("In server.restart()");
    executor.killAllConnections();
    stackManager.restart();
  }

  // This method should be ran in a loop to continually accept new connections
  public void run() throws IOException {
    Socket connection = inputSocket.accept();

    if(executor.threadsAvailable()){
      // Proceed as normal
      executor.execute(new ConnectionHandler(connection, stackManager));
    } else {
      // We dont want to handle more than 100 connections at once.
      // If the request queue is full & the oldest thread is slow, we should kill the oldest thread to avoid deadlocks.
      if(executor.hasSlowThread()){
        // The oldest thread has been running for 10 or more seconds, let's remove it
        executor.killOldestHandler();
        executor.execute(new ConnectionHandler(connection, stackManager));
      } else {
        // We're busy processing stuff and nothing has been marked slow, come back a little later
        connection.getOutputStream().write(0xff);
        connection.close();
      }
    }
  }

  // This method will set up a socket to listen for status requests on port 8081
  private void setupStatusSocket(int port) {
    // Lets catch this here so we dont kill the program if 8081 is in use
    try {
      statusSocket = new ServerSocket(port);
    } catch(Exception e){
      System.out.println("Error setting up status socket! Is port 8081 available? " + e.getMessage());
    }

    // thread to handle status requests
    Runnable statusConnection = new Runnable(){
      @Override
      public void run() {
        try {
          while(true){
            Socket connection = statusSocket.accept();
            System.out.println("New status connection created");

            // get & write the status
            String status = stackManager.printStatus();
            PrintWriter statusWriter = new PrintWriter(connection.getOutputStream());
            statusWriter.write(status);

            // Listen for different status request types
            byte serverAction = (byte)connection.getInputStream().read();
            if(serverAction == 0xff){
              // kill all connections & clean the stack
              executor.killAllConnections();
              stackManager.restart();
            }

            connection.close();
          }
        } catch(Exception e){
          e.printStackTrace();
        }
      }
    };

    // set up a singleThreadExecutr to manage this listener
    Executor statusExecutor = Executors.newSingleThreadExecutor();
    statusExecutor.execute(statusConnection);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Server mainServer = new Server();
    System.out.println("Server running on port 8080!");
    // Keep accepting connections
    while(true){
      mainServer.run();
    }
  }
}

