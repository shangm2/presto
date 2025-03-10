#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sys/stat.h>

// Boost includes
#include <boost/shared_ptr.hpp>

// Thrift includes
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

// Generated code
#include "gen-cpp/test_types.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace test;

// Serialize a TestMessage to string
string serialize(const TestMessage& message) {
    // Create a memory buffer transport
    std::shared_ptr<TMemoryBuffer> memoryBuffer = std::make_shared<TMemoryBuffer>();
    
    // Create a binary protocol using the memory buffer transport
    std::shared_ptr<TBinaryProtocol> protocol = std::make_shared<TBinaryProtocol>(memoryBuffer);
    
    // Write the message to the protocol
    message.write(protocol.get());
    
    // Get the serialized data
    uint8_t* buffer;
    uint32_t size;
    memoryBuffer->getBuffer(&buffer, &size);
    
    return string((char*)buffer, size);
}

// Deserialize a string to TestMessage
TestMessage deserialize(const string& data) {
    // Create a memory buffer with the input data
    std::shared_ptr<TMemoryBuffer> memoryBuffer = std::make_shared<TMemoryBuffer>(
        (uint8_t*)data.data(), data.size());
    
    // Create a binary protocol using the memory buffer
    std::shared_ptr<TBinaryProtocol> protocol = std::make_shared<TBinaryProtocol>(memoryBuffer);
    
    // Read the message from the protocol
    TestMessage message;
    message.read(protocol.get());
    return message;
}

// Create directory if it doesn't exist
void createDirectoryIfNotExists(const string& path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        // Directory doesn't exist, create it
#ifdef _WIN32
        _mkdir(path.c_str());
#else
        mkdir(path.c_str(), 0755);
#endif
    }
}

// Save string to file
void saveToFile(const string& data, const string& filename) {
    createDirectoryIfNotExists("/tmp/thrift");

    string fullPath = "/tmp/thrift/" + filename;
    ofstream file(fullPath, ios::binary);
    if (file.is_open()) {
        file.write(data.c_str(), data.size());
        file.close();
        cout << "Saved data to " << fullPath << endl;
    } else {
        cerr << "Could not open file for writing: " << fullPath << endl;
    }
}

// Read string from file
string readFromFile(const string& filename) {
    string fullPath = "/tmp/thrift/" + filename;
    ifstream file(fullPath, ios::binary | ios::ate);
    if (file.is_open()) {
        streamsize size = file.tellg();
        file.seekg(0, ios::beg);
        
        vector<char> buffer(size);
        if (file.read(buffer.data(), size)) {
            file.close();
            return string(buffer.data(), size);
        }
        file.close();
    }
    cerr << "Could not read file: " << fullPath << endl;
    return "";
}

int main() {
    try {
        // Create a TestMessage
        TestMessage message;
        message.text = "Hello from C++!";
        message.number = 100;
        
        vector<string> items;
        items.push_back("First item");
        items.push_back("Second item");
        items.push_back("Third item");
        message.items = items;
        
        // Serialize to string
        string serialized = serialize(message);
        cout << "Serialized message to " << serialized.size() << " bytes" << endl;
        
        // Save to file (so Java can read it)
        saveToFile(serialized, "message_from_cpp.bin");
        
        // Read from file (simulating reading from Java)
        string readData = readFromFile("message_from_java.bin");
        if (!readData.empty()) {
            TestMessage receivedMessage = deserialize(readData);
            cout << "Received message from Java:" << endl;
            cout << "  Text: " << receivedMessage.text << endl;
            cout << "  Number: " << receivedMessage.number << endl;
            cout << "  Items: ";
            for (const auto& item : receivedMessage.items) {
                cout << item << ", ";
            }
            cout << endl;
        } else {
            cout << "No data received from Java (file not found or empty)" << endl;
        }
        
    } catch (exception& e) {
        cerr << "ERROR: " << e.what() << endl;
    }
    
    return 0;
}