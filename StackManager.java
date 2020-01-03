package stackserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class StackManager  {
  private Deque<ByteBuffer> stack;
  private Queue<ConnectionHandler> queue;

  public StackManager() {
    stack = new LinkedBlockingDeque<>(100);
    queue = new LinkedBlockingQueue<>();
  }

  public void push(ByteBuffer payload) throws IOException, InterruptedException {
    // Set the byte buffer up for reading
    payload.flip();
    stack.push(payload);

    // If there's a connection in the queue, dequeue it to unlock its thread
    if(!queue.isEmpty()){
      dequeue();
    }
  }

  public ByteBuffer pop() throws IOException, InterruptedException {
    ByteBuffer returnVal = stack.pop();

    // if there's a connection in the queue, dequeue it to unlock its thread
    if(!queue.isEmpty()){
      dequeue();
    }

    return returnVal;
  }

  public void enqueue(ConnectionHandler handler) {
    queue.add(handler);
  }

  public void dequeue() throws IOException, InterruptedException {
    ConnectionHandler handler = queue.remove();
    // countdown the latch
    handler.countDown();
  }

  public void restart(){
    // clear the stack & queue
    System.out.println("Cleaning out the stack & queue");
    stack.clear();
    queue.clear();
  }

  public String printStatus(){
    String statusString = "The size of the stack:" + getSize() + "\n"
                        + "The size of the queue:" + getQueueSize() + "\n";

    System.out.println("The statusString " + statusString);
    return statusString;
  }

  public boolean isFull() {
    boolean isFullBool = (stack.size() == 100);
    return isFullBool;
  }

  public boolean isEmpty() {
    boolean isEmptyBool = stack.isEmpty();
    return isEmptyBool;
  }

  public int getSize() {
    return stack.size();
  }

  public int getQueueSize() {
    return queue.size();
  }
}
