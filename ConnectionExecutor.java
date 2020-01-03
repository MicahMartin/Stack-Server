package stackserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConnectionExecutor extends ThreadPoolExecutor {
  private static List<ConnectionHandler> connectionList = Collections.synchronizedList(new ArrayList<ConnectionHandler>());

  // Set up a threadpool with a limit set through the connectionLimit arg
  public ConnectionExecutor(int connectionLimit) {
    super(connectionLimit, connectionLimit, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  }

  public void killAllConnections() {
    for (ConnectionHandler handler : connectionList) {
      System.out.println("Killing connections");
      handler.interrupt();
    }
  }

  public void killOldestHandler(){
    connectionList.get(0).interrupt();
  }

  public int getOldestHandlerRuntime(){
    long timeNow = System.currentTimeMillis();
    long oldestThreadTime = connectionList.get(0).getRequestStartTime();

    int diff = (int)((timeNow - oldestThreadTime)/1000);
    return diff;
  }

  public boolean threadsAvailable(){
    if (getActiveCount() >= 100) return false;
    // else
    return true;
  }
  
  public boolean hasSlowThread(){
    if(getOldestHandlerRuntime() >= 10) return true;
    return false;
  }

  public ConnectionHandler getHandler(int index) {
    ConnectionHandler handler = connectionList.get(index);
    return handler;
  }

  // Parent class {@link ThreadPoolExecutor} provides 'hook' methods that fire before and after a thread executes.
  // We just want to keep track of the connection reference with a list for now.
  @Override
  protected synchronized void beforeExecute(Thread t, Runnable connection) {
    connectionList.add((ConnectionHandler)connection);
  }

  @Override
  protected synchronized void afterExecute(Runnable connection, Throwable throwable) {
    connectionList.remove((ConnectionHandler)connection);
  }
};


