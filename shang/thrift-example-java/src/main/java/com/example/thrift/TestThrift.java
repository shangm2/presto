package com.example.thrift;

import com.example.thrift.generated.TestMessage;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestThrift {
    public static void main(String[] args) {
        try {
            // Create a TestMessage
            TestMessage message = new TestMessage();
            message.setText("Hello from Java!");
            message.setNumber(42);
            
            List<String> items = new ArrayList<>();
            items.add("Item 1");
            items.add("Item 2");
            items.add("Item 3");
            message.setItems(items);
            
            // Serialize to bytes
            byte[] serialized = serialize(message);
            System.out.println("Serialized message to " + serialized.length + " bytes");
            
            // Save to file (so C++ can read it)
            saveToFile(serialized, "message_from_java.bin");
            System.out.println("Saved serialized message to message.bin");
            
            // Read from file (simulating reading from C++)
            byte[] readData = readFromFile("message_from_cpp.bin");
            if (readData != null && readData.length > 0) {
                TestMessage receivedMessage = deserialize(readData);
                System.out.println("Received message from C++:");
                System.out.println("  Text: " + receivedMessage.getText());
                System.out.println("  Number: " + receivedMessage.getNumber());
                System.out.println("  Items: " + receivedMessage.getItems());
            } else {
                System.out.println("No data received from C++ (file not found or empty)");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Serialize a TestMessage to byte array
    public static byte[] serialize(TestMessage message) throws TException {
        TMemoryBuffer transport = new TMemoryBuffer(1024);
        TProtocol protocol = new TBinaryProtocol(transport);
        message.write(protocol);
        return transport.getArray();
    }
    
    // Deserialize a byte array to TestMessage
    public static TestMessage deserialize(byte[] data) throws TException {
        TMemoryInputTransport transport = new TMemoryInputTransport(data);
        TProtocol protocol = new TBinaryProtocol(transport);
        TestMessage message = new TestMessage();
        message.read(protocol);
        return message;
    }
    
    // Save byte array to file
    public static void saveToFile(byte[] data, String filename) throws IOException {

        // Create directory if it doesn't exist
        File directory = new File("/tmp/thrift");
        if (!directory.exists()) {
            directory.mkdirs();
        }
        String fullPath = "/tmp/thrift/" + filename;

        try (FileOutputStream fos = new FileOutputStream(fullPath)) {
            fos.write(data);
            System.out.println("Saved data to " + fullPath);
        }
    }
    
    // Read byte array from file
    public static byte[] readFromFile(String filename) {
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] data = new byte[fis.available()];
            fis.read(data);
            return data;
        } catch (IOException e) {
            System.out.println("Could not read file: " + e.getMessage());
            return null;
        }
    }
}

