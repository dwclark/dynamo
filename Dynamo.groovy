import java.util.function.Consumer
import java.nio.ByteBuffer
import java.util.Map.Entry
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.pagination.sync.SdkIterable
import software.amazon.awssdk.core.waiters.WaiterResponse
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter
import software.amazon.awssdk.utils.builder.SdkBuilder

class Dynamo {

    private <T> T delegateTo(T obj, Closure config) {
	config.setDelegate(obj)
	config.setResolveStrategy(Closure.DELEGATE_FIRST)
	config.call()
	return obj
    }

    private SdkBuilder builder(Class type, Closure closure) {
	return delegateTo(type.builder(), closure)
    }
    
    enum NumberConversion {
	DECIMAL, DOUBLE, SMALLEST
    }

    private ScalarAttributeType scalars(Class type) {
	if(type === String)
	    return ScalarAttributeType.S
	else if(type === Number)
	    return ScalarAttributeType.N
	else if(type === ByteBuffer)
	    return ScalarAttributeType.B
	else
	    throw new IllegalArgumentException("${type} is not a legal scalar type")
    }
    
    final DynamoDbClient client
    private final NumberConversion numConv
    
    private final Map<AttributeValue.Type,Closure> conversions =
    	new EnumMap((AttributeValue.Type.B): { av -> av.b().asByteBuffer() },
		    (AttributeValue.Type.BOOL): { av -> av.bool() },
		    (AttributeValue.Type.BS): { av -> av.bs().inject(new LinkedHashSet()) { s, v -> s << s.asByteBuffer() } },
		    (AttributeValue.Type.L): { av -> av.l().collect { unwrap(it) } },
		    (AttributeValue.Type.M): { av -> av.m().inject([:]) { accum, k, v -> accum << new MapEntry(k, unwrap(v)) } },
		    (AttributeValue.Type.N): { av -> number(av.n()) },
		    (AttributeValue.Type.NS): { av -> av.ns().inject(new LinkedHashSet()) { s, v -> s << number(v) } },
		    (AttributeValue.Type.NUL): { av -> av.nul() },
		    (AttributeValue.Type.S): { av -> av.s() },
		    (AttributeValue.Type.SS): { av -> new LinkedHashSet(av.ss()) })
    
    Dynamo(DynamoDbClient client, NumberConversion nc = NumberConversion.DECIMAL) {
	this.client = client
	this.numConv = nc
    }
    
    Dynamo newConversions(NumberConversion numConv) {
	new Dynamo(client, numConv)
    }
    
    private AttributeValue attr(Boolean val) {
	return AttributeValue.builder().bool(val).build()
    }

    private AttributeValue attr(byte[] val) {
	return AttributeValue.builder().b(SdkBytes.fromByteArrayUnsafe(val)).build()
    }
    
    private AttributeValue attr(ByteBuffer val) {
	return AttributeValue.builder().b(SdkBytes.fromByteBuffer(val)).build()
    }
    
    private AttributeValue attr(CharSequence val) {
	return AttributeValue.builder().s(val.toString()).build()
    }

    private AttributeValue attr(Number val) {
	return AttributeValue.builder().n(val.toString()).build()
    }

    private AttributeValue attr(UUID val) {
	return AttributeValue.builder().s(val.toString()).build()
    }

    private AttributeValue attr(Set set) {
	if(set.every { it instanceof CharSequence})
	    return AttributeValue.builder().ss(set.collect { it.toString() })
	else if(set.every { it instanceof Number })
	    return AttributeValue.builder().ns(set.collect { it.toString() })
	else if(set.every { it instanceof ByteBuffer })
	    return AttributeValue.builder().bs(set.collect { SdkBytes.fromByteBuffer(it) })
	else 
	    return attr(new ArrayList(set))
    }

    private AttributeValue attr(List list) {
	return AttributeValue.builder().l(list.collect { val -> attr(val) })
    }
    
    private AttributeValue attr(Map<String,?> map) {
	return AttributeValue.builder().m(map.inject([:]) { accum, k, v -> accum << new MapEntry(k, attr(v)) })
    }

    private AttributeValueUpdate attrPut(Object any) {
	return AttributeValueUpdate.builder()
	    .value(attr(any))
	    .action(AttributeAction.PUT)
	    .build()
    }

    private Class smallest(BigDecimal bd) {
	if(bd.scale() > 0)
	    return BigDecimal
	else if(bd < Integer.MAX_VALUE)
	    return Integer
	else if(bd < Long.MAX_VALUE)
	    return Long
	else
	    return BigInteger
    }

    private Number number(String s) {
	final BigDecimal bd = new BigDecimal(s)
	if(numConv === NumberConversion.DECIMAL) return bd
	else if(numConv == NumberConversion.DOUBLE) return bd.doubleValue()
	else {
	    final Class type = smallest(bd)
	    if(type === BigDecimal)
		return bd
	    else if(type === BigInteger)
		return bd.toBigInteger()
	    else if(type === Long)
		return bd.longValue()
	    else
		return bd.intValue()
	}
    }
    
    private Object unwrap(AttributeValue av) {
	return conversions[av.type()].call(av)
    }

    private Map<String,Object> unwrap(Map<String,AttributeValue> vals) {
	return vals.inject([:]) { accum, k, v -> accum << new MapEntry(k, unwrap(v)) }
    }

    private Map<String,AttributeValue> wrap(Map<String,Object> vals) {
	return vals.inject([:]) { accum, k, v -> accum << new MapEntry(k, attr(v)) }
    }

    private class DynamoIterable implements Iterable<Map<String,Object>> {
	private final SdkIterable<Map<String,Object>> sdkIter

	DynamoIterable(SdkIterable<Map<String,Object>> sdkIter) {
	    this.sdkIter = sdkIter
	}
	
	Iterator<Map<String,Object>> iterator() {
	    final Iterator<Map<String,Object>> sdkIterator = sdkIter.iterator()
	    return new Iterator() {
		boolean hasNext() { sdkIterator.hasNext() }
		Map<String,Object> next() { unwrap(sdkIterator.next()) }
	    }
	}
    }

    private class TableImpl implements Ops.Table {
	private final String __table
	
	TableImpl(final String table) {
	    __table = table
	}
	
	void put(Map<String,Object> map) {
	    def req = builder(PutItemRequest) {
		tableName __table
		item wrap(map)
	    }

	    client.putItem(req.build())
	}

	private void _put(Ops.All all) {
	    def req = builder(PutItemRequest) {
		tableName __table
		if(all.__condition)
		    conditionExpression all.__condition
		item wrap(all.__attributes)
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }

	    client.putItem(req.build())
	}
	
	void put(Closure config) {
	    _put(Ops.delegateAll(config))
	}

	void put(Consumer<Ops.Put> config) {
	    _put(Ops.noDelegate(config))
	}
	
	Map<String,Object> get(Map<String,Object> keys) {
	    def req = builder(GetItemRequest) {
		tableName __table
		key wrap(keys)
	    }

	    unwrap(client.getItem(req.build()).item())
	}

	private Map<String,Object> _get(Ops.All all) {
	    def req = builder(GetItemRequest) {
		tableName __table
		key wrap(all.__key)
		if(all.__projection)
		    projectionExpression all.__projection
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }

	    unwrap(client.getItem(req.build()).items())
	}

	Map<String,Object> get(Closure config) {
	    _get(Ops.delegateAll(config))
	}

	Map<String,Object> get(Consumer<Ops.Get> config) {
	    _get(Ops.noDelegate(config))
	}

	private void _upsert(Ops.All all) {
	    all.convertAttributesToExpression()
	    
	    final b = builder(UpdateItemRequest) {
		tableName __table
		key wrap(all.__key)
		updateExpression all.__expression
		if(all.__condition)
		    conditionExpression all.__condition
		if(all.__aliases)
		    expressionAttributeNames(all.__aliases)
		if(all.__params) {
		    expressionAttributeValues wrap(all.__params)
		}
	    }

	    client.updateItem(b.build())
	}
	
	void upsert(Closure config) {
	    _upsert(Ops.delegateAll(config))
	}

	void upsert(Consumer<Ops.Upsert> config) {
	    _upsert(Ops.noDelegate(config))
	}

	private Iterable<Map<String,Object>> _query(Ops.All all) {
	    def req = builder(QueryRequest) {
		tableName __table
		keyConditionExpression all.__keyCondition
		if(all.__index)
		    indexName all.__index
		if(all.__projection)
		    projectionExpression all.__projection
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }
	    
	    return new DynamoIterable(client.queryPaginator(req.build()).items())
	}
	
	Iterable<Map<String,Object>> query(Closure config) {
	    _query(Ops.delegateAll(config))
	}

	Iterable<Map<String,Object>> query(Consumer<Ops.Query> config) {
	    _query(Ops.noDelegate(config))
	}
	
	Iterable<Map<String,Object>> scan() {
	    return scan { -> }
	}

	private Iterable<Map<String,Object>> _scan(Ops.All all) {
	    def req = builder(ScanRequest) {
		tableName __table
		if(all.__index)
		    indexName all.__index
		if(all.__projection)
		    projectionExpression all.__projection
		if(all.__filter)
		    filterExpression all.__filter
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }

	    return new DynamoIterable(client.scanPaginator(req.build()).items())
	}
	
	Iterable<Map<String,Object>> scan(Closure config) {
	    _scan(Ops.delegateAll(config))
	}

	Iterable<Map<String,Object>> scan(Consumer<Ops.Scan> config) {
	    _scan(Ops.noDelegate(config))
	}

	void delete(Map<String,Object> keys) {
	    def req = builder(DeleteItemRequest) {
		tableName __table
		key wrap(keys)
	    }

	    client.deleteItem(req.build())
	}
	
	private void _delete(Ops.All all) {
	    def req = builder(DeleteItemRequest) {
		tableName __table
		key wrap(all.__keys)
		if(all.__condition)
		    conditionExpression all.__condition
		if(all.__aliases)
		    expressionAttributeNames(all.__aliases)
		if(all.__params) {
		    expressionAttributeValues wrap(all.__params)
		}
	    }
	    
	    client.deleteItem(req.build())
	}

	void delete(Closure config) {
	    _delete(Ops.delegateAll(config))
	}

	void delete(Consumer<Ops.Delete> config) {
	    _delete(Ops.noDelegate(config))
	}

	void delete() {
	    def req = builder(DeleteTableRequest) {
		tableName __table
	    }

	    client.deleteTable(req.build())
	}
    }
    
    Ops.Table forTable(final String tableName) {
	return new TableImpl(tableName)
    }

    List<String> listTables() { 
	def request = ListTablesRequest.builder().build()
	return client.listTables(request).tableNames()
    }
    
    Ops.Table createTable(String name, List<Entry<String,Class>> keys) {
	DynamoDbWaiter dbWaiter = client.waiter()
	def attrDefs = keys.collect { e ->
	    AttributeDefinition.builder().attributeName(e.key).attributeType(scalars(e.value)).build()
	}
	
	int index = 0
	def schemas = keys.collect { e ->
	    KeySchemaElement.builder().attributeName(e.key).keyType(index++ ? KeyType.RANGE : KeyType.HASH).build()
	}
	
	def ctb = builder(CreateTableRequest) {
	    attributeDefinitions attrDefs
	    keySchema schemas
	    billingMode BillingMode.PAY_PER_REQUEST
	    tableName name
	}

	client.createTable(ctb.build())
	
	def tb = builder(DescribeTableRequest) {
            tableName name
	}
	
	// Wait until the Amazon DynamoDB table is created.
	def roe = dbWaiter.waitUntilTableExists(tb.build()).matched()
	if(roe.exception().present)
	    throw roe.exception().get()

	return new TableImpl(name)
    }

    private class Read implements Ops.ReadTransaction {
	final List<TransactGetItem> items = []

	void get(Map<String,Object> keys, String table) {
	    def req = builder(Get) {
		tableName table
		key wrap(keys)
	    }
	    
	    items << builder(TransactGetItem) { get req.build() }.build()
	}

	void _get(Ops.All all) {
	    def ret = builder(Get) {
		tableName __table
		key wrap(all.__key)
		if(all.__projection)
		    projectionExpression all.__projection
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }

	    items << builder(TransactGetItem) { get req.build() }.build()
	}
	
	void get(Closure config) {
	    _get(Ops.delegateAll(config))
	}

	void get(Consumer<Ops.Get> consumer) {
	    _get(Ops.noDelegate(consumer))
	}
    }

    private class Write implements Ops.WriteTransaction {
	final List<TransactWriteItem> items = []
	final UUID token = UUID.randomUUID()

	private void _check(Ops.All all) {
	    final b = builder(ConditionCheck) {
		tableName all.__table
		key wrap(all.__key)
		if(all.__condition)
		    conditionExpression all.__condition
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }

	    items << builder(TransactWriteItem) { conditionCheck b.build() }.build()

	}
	
	void check(Closure config) {
	    _check(Ops.delegateAll(config))
	}

	void check(Consumer<Ops.Check> consumer) {
	    _check(Ops.noDelegate(consumer))
	}
	
	void put(Map<String,Object> map, String table) {
	    final b = builder(Put) {
		tableName table
		item wrap(map)
	    }

	    items << builder(TransactWriteItem) { put b.build() }.build()
	}

	private void _put(Ops.All all) {
	    final b = builder(Put) {
		tableName all.__table
		item wrap(all.__attributes)
		if(all.__condition)
		    conditionExpression all.__condition
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }

	    items << builder(TransactWriteItem) { put b.build() }.build()
	}
	
	void put(Closure config) {
	    _put(Ops.delegateAll(config))
	    
	}

	void put(Consumer<Ops.Put> consumer) {
	    _put(Ops.noDelegate(consumer))
	}

	private void _upsert(Ops.All all) {
	    final b = builder(Update) {
		tableName all.__table
		key wrap(all.__key)
		updateExpression all.__expression
		if(all.__condition)
		    conditionExpression all.__condition
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }
	    
	    items << builder(TransactWriteItem) { update b.build() }.build()
	}

	void upsert(Consumer<Ops.Upsert> consumer) {
	    _upsert(Ops.noDelegate(consumer).convertAttributesToExpression())
	}
	
	void upsert(Closure config) {
	    _upsert(Ops.delegateAll(config).convertAttributesToExpression())
	}
	
	void delete(Map<String,Object> keys, String table) {
	    final b = builder(Delete) {
		tableName table
		key wrap(keys)
	    }

	    items << builder(TransactWriteItem) { delete b.build() }.build()
	}

	private void _delete(Ops.All all) {
	    final b = builder(Delete) {
		tableName all.__table
		key wrap(all.__key)
		if(all.__condition)
		    conditionExpression all.__condition
		if(all.__aliases)
		    expressionAttributeNames all.__aliases
		if(all.__params)
		    expressionAttributeValues wrap(all.__params)
	    }

	    items << builder(TransactWriteItem) { delete b.build() }.build()
	}
	
	void delete(Closure config) {
	    _put(Ops.delegateAll(config))
	}
	
	void delete(Consumer<Ops.Delete> consumer) {
	    _delete(Ops.noDelegate(consumer))
	}
    }

    List<Map<String,Object>> _readTransaction(Read read) {
	final req = builder(TransactGetItemsRequest) { transactItems read.items }.build()
	client.transactGetItems(req).responses().collect { unwrap(it.item()) }
    }
    
    List<Map<String,Object>> readTransaction(@DelegatesTo(Ops.ReadTransaction) Closure config) {
	_readTransaction(delegateTo(new Read(), config))
    }

    List<Map<String,Object>> readTransaction(Consumer<Ops.ReadTransaction> config) {
	final Read read = new Read()
	config.accept(read)
	_readTransaction(read)
    }
    
    private void _writeTransaction(Write write) {
	final b = builder(TransactWriteItemsRequest) {
	    clientRequestToken write.token.toString()
	    transactItems write.items
	}
	
	client.transactWriteItems(b.build())
    }
    
    void writeTransaction(@DelegatesTo(Ops.WriteTransaction) Closure config) {
	_writeTransaction(delegateTo(new Write(), config))
    }

    void writeTransaction(Consumer<Ops.WriteTransaction> consumer) {
	final Write write = new Write()
	consumer.accept(write)
	_writeTransaction(write)
    }
}
