import java.nio.ByteBuffer
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.waiters.WaiterResponse
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter
import software.amazon.awssdk.core.pagination.sync.SdkIterable
import java.util.Map.Entry

class Dynamo {

    private Object build(Class type, Closure closure) {
	def builder = type.builder()
	closure.setDelegate(builder)
	closure.setResolveStrategy(Closure.DELEGATE_FIRST)
	closure.call(builder)
	return builder.build()
    }
    
    private Object exec(Class type, Closure closure) {
	def req = build(type, closure)
	Closure func = reqMap[type]
	func.call(req)
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
    
    private final Map<Class,Closure> reqMap =
	Map.copyOf((UpdateItemRequest): client.&updateItem,
		   (GetItemRequest): client.&getItem,
		   (PutItemRequest): client.&putItem,
		   (QueryRequest): client.&query,
		   (DeleteTableRequest): client.&deleteTable,
		   (CreateTableRequest): client.&createTable)
    
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

    static Map<String,String> aliases(Map<String,Object> map) {
	Map<String,String> ret = [:]
	map.eachWithIndex { k, v, i -> ret["#a" + i] = k }
	return ret
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

    List<String> listTables() { 
	def request = ListTablesRequest.builder().build()
	return client.listTables(request).tableNames()
    }
    
    void createTable(String name, List<Entry<String,Class>> keys) {
	DynamoDbWaiter dbWaiter = client.waiter()
	def attrDefs = keys.collect { e ->
	    AttributeDefinition.builder().attributeName(e.key).attributeType(scalars(e.value)).build()
	}

	int index = 0
	def schemas = keys.collect { e ->
	    KeySchemaElement.builder().attributeName(e.key).keyType(index++ ? KeyType.RANGE : KeyType.HASH).build()
	}
	
	def resp = exec(CreateTableRequest) {
	    attributeDefinitions attrDefs
	    keySchema schemas
	    billingMode BillingMode.PAY_PER_REQUEST
	    tableName name
	}

	def tableRequest = DescribeTableRequest.builder()
            .tableName(name)
            .build()
	
	// Wait until the Amazon DynamoDB table is created.
	def roe = dbWaiter.waitUntilTableExists(tableRequest).matched()
	if(roe.exception().present)
	    throw roe.exception().get()
    }

    void put(String table, Map<String,Object> map) {
	exec(PutItemRequest) {
            tableName table
	    item wrap(map)
	}
    }

    Map<String,Object> get(String table, Map<String,Object> keys) {
	def resp = exec(GetItemRequest) {
	    key wrap(keys)
	    tableName table
	}

	return unwrap(resp.item())
    }

    
    void upsert(String table, @DelegatesTo(SmartUpsert) Closure config) {
	SmartUpsert sa = new SmartUpsert()
	config.setDelegate(sa)
	config.setResolveStrategy(Closure.DELEGATE_FIRST)
	config.call()
	
	exec(UpdateItemRequest) {
	    tableName table
	    key wrap(sa.__key)
	    updateExpression sa.__updateExpression
	    if(sa.__aliases)
		expressionAttributeNames(sa.__aliases)
	    if(sa.__params) {
		expressionAttributeValues wrap(sa.__params)
	    }
	}
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

    Iterable<Map<String,Object>> query(String table, @DelegatesTo(SmartQuery) Closure config) {
	final SmartQuery sq = new SmartQuery()
	config.setDelegate(sq)
	config.setResolveStrategy(Closure.DELEGATE_FIRST)
	config.call()
	
	def req = build(QueryRequest) {
	    tableName table
	    keyConditionExpression sq.__expr
	    if(sq.__index)
		indexName sq.__index
	    if(sq.__projection)
		projectionExpression sq.__projection
	    if(sq.__aliases)
		expressionAttributeNames sq.__aliases
	    if(sq.__params)
		expressionAttributeValues wrap(sq.__params)
	}

	return new DynamoIterable(client.queryPaginator(req).items())
    }

    Iterable<Map<String,Object>> scan(String table) {
	return scan(table, { -> })
    }

    Iterable<Map<String,Object>> scan(String table, @DelegatesTo(SmartScan) Closure config) {
	final SmartScan ss = new SmartScan()
	config.setDelegate(ss)
	config.setResolveStrategy(Closure.DELEGATE_FIRST)
	config.call()
	
	def req = build(ScanRequest) {
	    tableName table
	    if(ss.__index)
		indexName ss.__index
	    if(ss.__projection)
		projectionExpression ss.__projection
	    if(ss.__filter)
		filterExpression ss.__filter
	    if(ss.__aliases)
		expressionAttributeNames ss.__aliases
	    if(ss.__params)
		expressionAttributeValues wrap(ss.__params)
	}

	return new DynamoIterable(client.scanPaginator(req).items())
    }

    void deleteTable(String table) {
	exec(DeleteTableRequest) {
	    tableName table
	}
    }
    
    interface Table {
	void put(Map<String,Object> map)
	Map<String,Object> get(Map<String,Object> keys)
	void upsert(@DelegatesTo(SmartUpsert) Closure config)
	Iterable<Map<String,Object>> query(@DelegatesTo(SmartQuery) Closure config)
	Iterable<Map<String,Object>> scan()
	Iterable<Map<String,Object>> scan(@DelegatesTo(SmartScan) Closure config)
	void delete()
    }
    
    Table forTable(final String tableName) {
	return new Table() {
	    void put(Map<String,Object> map) {
		put(tableName, map)
	    }
	    
	    Map<String,Object> get(Map<String,Object> keys) {
		return get(tableName, keys)
	    }
	    
	    void upsert(Closure config) {
		upsert(tableName, config)
	    }

	    Iterable<Map<String,Object>> query(Closure config) {
		return query(tableName, config)
	    }

	    Iterable<Map<String,Object>> scan(Closure config = { -> }) {
		return scan(tableName, config)
	    }
	    
	    void delete() {
		deleteTable(tableName)
	    }
	}
    }
}
