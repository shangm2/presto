
getCodec(TaskUpdateRequest.class)
    │
    └── ThriftCodecByteCodeGenerator for TaskUpdateRequest
            │
            └── declareCodecFields() loops through fields:
                    │
                    ├── Field: List<TaskSource> sources
                    │       └── getCodec(List<TaskSource>)
                    │               └── Creates ListThriftCodec
                    │                       └── getCodec(TaskSource.class)  ⬅️ RECURSIVE!
                    │                               └── ThriftCodecByteCodeGenerator for TaskSource
                    │                                       └── declareCodecFields()
                    │                                               └── getCodec(List<ScheduledSplit>)
                    │                                                       └── getCodec(ScheduledSplit.class) ⬅️ RECURSIVE!
                    │                                                               └── ...continues...
                    │
                    ├── Field: OutputBuffers outputBuffers
                    │       └── getCodec(OutputBuffers.class)  ⬅️ RECURSIVE!
                    │               └── ThriftCodecByteCodeGenerator for OutputBuffers
                    │
                    ├── Field: Optional<TableWriteInfo> tableWriteInfo
                    │       └── getCodec(Optional<TableWriteInfo>)
                    │               └── getCodec(TableWriteInfo.class)  ⬅️ RECURSIVE!
                    │
                    └── ... more fields ...


@ThriftStruct
public class Order {
    @ThriftField(1) public String id;           // primitive - no codec needed
    @ThriftField(2) public Customer customer;   // nested struct - needs codec!
    @ThriftField(3) public List<Item> items;    // collection - needs codec!
}

public class OrderCodec implements ThriftCodec<Order> {

    // ==================== FIELDS (from declareTypeField + declareCodecFields) ====================
    
    private final ThriftType type;                           // From declareTypeField()
    private final ThriftCodec<Customer> customerCodec;       // From declareCodecFields() - nested struct
    private final ThriftCodec<List<Item>> itemsCodec;        // From declareCodecFields() - collection

    // ==================== CONSTRUCTOR (from defineConstructor) ====================
    
    public OrderCodec(
            ThriftType type,
            ThriftCodec<Customer> customerCodec,
            ThriftCodec<List<Item>> itemsCodec) {
        super();
        this.type = type;
        this.customerCodec = customerCodec;
        this.itemsCodec = itemsCodec;
    }

    // ==================== getType() (from defineGetTypeMethod) ====================
    
    @Override
    public ThriftType getType() {
        return this.type;
    }

    // ==================== read() (from defineReadStructMethod) ====================
    
    @Override
    public Order read(TProtocolReader protocol) throws Exception {
        ProtocolReader reader = new ProtocolReader(protocol);
        
        // Declare local variables for each field (initialized to defaults)
        String f_id = null;
        Customer f_customer = null;
        List<Item> f_items = null;
        
        // Read struct
        reader.readStructBegin();
        
        while (reader.nextField()) {
            switch (reader.getFieldId()) {
                case 1:  // id (String - primitive, read directly)
                    f_id = reader.readString();
                    break;
                    
                case 2:  // customer (nested struct - use delegate codec)
                    f_customer = customerCodec.read(reader);
                    break;
                    
                case 3:  // items (collection - use delegate codec)
                    f_items = itemsCodec.read(reader);
                    break;
                    
                default:
                    reader.skipFieldData();
                    break;
            }
        }
        
        reader.readStructEnd();
        
        // Build the struct instance
        Order result = new Order();
        result.id = f_id;
        result.customer = f_customer;
        result.items = f_items;
        
        return result;
    }

    // ==================== write() (from defineWriteStructMethod) ====================
    
    @Override
    public void write(Order struct, TProtocolWriter protocol) throws Exception {
        ProtocolWriter writer = new ProtocolWriter(protocol);
        
        writer.writeStructBegin("Order");
        
        // Field 1: id (String - primitive, write directly)
        if (struct.id != null) {
            writer.writeStringField("id", (short) 1, struct.id);
        }
        
        // Field 2: customer (nested struct - use delegate codec)
        if (struct.customer != null) {
            writer.writeStructField("customer", (short) 2, customerCodec, struct.customer);
        }
        
        // Field 3: items (collection - use delegate codec)
        if (struct.items != null) {
            writer.writeListField("items", (short) 3, itemsCodec, struct.items);
        }
        
        writer.writeStructEnd();
    }

    // ==================== BRIDGE METHODS (from defineReadBridgeMethod + defineWriteBridgeMethod) ====================
    
    // Bridge method for type erasure - read
    @Override
    public /*bridge synthetic*/ Object read(TProtocolReader protocol) throws Exception {
        return this.read(protocol);  // Calls Order read() above
    }
    
    // Bridge method for type erasure - write
    @Override
    public /*bridge synthetic*/ void write(Object struct, TProtocolWriter protocol) throws Exception {
        this.write((Order) struct, protocol);  // Casts and calls Order write() above
    }
}

