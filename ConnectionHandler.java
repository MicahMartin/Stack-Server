package stackserver;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConnectionHandler implements Runnable {
  private final CountDownLatch latch = new CountDownLatch(1);
  private static final ExecutorService pollingThreadExecutor = Executors.newFixedThreadPool(100);
  private StackManager stackManager;
  private Thread currentThread;
  private Socket connection;
  private long requestStart;
  private String threadName;
  private boolean interrupted;
  private byte header;

  public ConnectionHandler(Socket _connection, StackManager _stackManager) {
    connection = _connection;
    stackManager = _stackManager;
    requestStart = System.currentTimeMillis();
  }

  public void interrupt() {
    // Set interupted to true and countdown the latch. an interrupted exception still needs to be thrown & caught
    interrupted = true;
    countDown();
  }

  public void await(final DataInputStream stream) throws IOException, InterruptedException {
    // We want to make sure we can tell when the client has disconnected
    // So lets read the client connection in a new thread
    // If we detect a disconnect, then interrupt this thread
    Runnable pollingThread = new Runnable(){
      @Override
      public void run() {
        try {
          int statusCode = stream.read();
          // end of input
          if( statusCode == -1 ){
            interrupt();
          }
        } catch(Exception e){
          // if anything bad happens trying to read the socket interrupt anyway
          interrupt();
          e.printStackTrace();
        }
      }
    };
    pollingThreadExecutor.submit(pollingThread);

    // We're blocking this thread here until countDown() gets called by stackManager.push/pop()...stackManager.dequeue() OR interrupt()
    // If countDown() gets called by interrupt, something went wrong and we should throw an error
    latch.await();
    if(interrupted == true){
      throw new InterruptedException();
    }
  }

  public void countDown() {
    latch.countDown();
  }

  private void cleanupConnection() {
    try {
      connection.close();
    } catch(Exception e){
      // We ran into trouble closing the connection, but we can proceed
      System.out.println(threadName + " Error closing conection! " + e.getMessage());
    }
  }

  private ByteBuffer read(DataInputStream inputStream) throws IOException, InterruptedException {
    int pushSize = header + 1; // The header is a signed int describing the length of payload. 
                               // Adding +1 to this to account for the header byte in the final payload.
    
    ByteBuffer buffer = ByteBuffer.allocate(pushSize);
    // Make sure the header is in the payload!
    buffer.put(header);

    boolean end = false;
    int data;
    // We could get interrupted by the main thread here if the threadcount > 100 & this read is slow. See Server.run()
    while(!end && !interrupted){
      data = inputStream.read();
      if(data == -1 || interrupted){
        end = true;
        // Bad read, the client didn't send all the data or we've been interrupted for being slow
        throw new InterruptedException();
      }

      // Put this byte into the buffer
      buffer.put((byte)data);
      // If we've read up to the length specified in the header, stop reading
      if(buffer.position() == pushSize){
        end = true;
      };
    }

    return buffer;
  }

  
  /** 
   * 
   */
  @Override
  public void run() {
    setThreadName();
    try{
      // Get the input stream from socket connection
      DataInputStream connInStream = new DataInputStream(connection.getInputStream());
      // Read the first byte to determine what kind of request we're dealing with
      header = connInStream.readByte();
      // First bit is the type, 
      //  if its 0, its a push request & the remaining 7 bits are the length of the value.
      //  if its 1, then we have a pop request. The rest of the byte is ignored
      int requestType = (header & (1<<7));

      // Array of bytes to be sent back to the client
      byte[] res = null;
      // We have a push here
      if(requestType == 0){
        // Read the input stream
        ByteBuffer buffer = read(connInStream);

        if(stackManager.isFull()){
          // Wait for space on the stack or a disconnect signal from the client
          stackManager.enqueue(this);
          await(connInStream);
        }

        // Space is available on the stack!
        System.out.println(threadName + " PUSH");
        stackManager.push(buffer);

        // return a single empty byte
        res = new byte[]{0};
      } else {
        // We have a pop here
        if(stackManager.isEmpty()){
          // block until another thread dequeues me or the client disconnects
          stackManager.enqueue(this);
          await(connInStream);
        }

        System.out.println(threadName + " POP");
        ByteBuffer poppedBytes = stackManager.pop();
        // Setup a byte array using poopedBytes.remaining() for the length
        res = new byte[poppedBytes.remaining()];
        // Send the poppedBytes into the response byte array
        poppedBytes.get(res);
      }

      // write the response
      OutputStream connOutStream = connection.getOutputStream();
      connOutStream.write(res);
      connOutStream.close();

    } catch(Exception e){
      // catch interrupts and closed sockets
      e.getMessage();
    } finally {
      try {
        cleanupConnection();
      } catch(Exception e){
        e.getMessage();
      }
    }
  }

  public long getRequestStartTime(){
    return requestStart;
  }

  public String getThreadName(){
    return threadName;
  }

  private void setThreadName(){
    currentThread = Thread.currentThread();
    threadName = "Thread:" + currentThread.getName();
  }
}
