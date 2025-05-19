//Although we have to import the local dependency to make sure
//the dynamo libraries are present, the Dynamo class fully hides
//all of the dynamo model. Because of this, the script can be
//written using nothing but groovy strings, closures, and standard
//java types such as Map, List, String, etc.
@Grab('com.amazonaws:DynamoDBLocal:2.6.0') // or at project level @Grab('software.amazon.awssdk:dynamodb:2.31.41')
import static java.util.Map.entry

//Manages a local dynamo environment. If you are hitting an actual Dynamo instance
//in AWS, you would just configure the client directly
def builder = DynamoEnvironment.builder().tap {
    port 12500
    shared false
    useGrapeClasspath()
}

def demoSingleKey = { Dynamo dynamo ->
    final Map customerKey = [customerId: UUID.randomUUID()]
    assert !dynamo.listTables()

    //Demo table creation with a single primary key
    dynamo.createTable("Customers", [entry("customerId", String)])

    //Use the table view to always do work with same table
    def table = dynamo.forTable("Customers")

    //put and get customer data, put is always an insert
    table.put(customerKey + [name: 'David', address: '123 Main', age: 100])
    println table.get(customerKey)

    //show upsert functionality, in this case since the key exists it will
    //be an update. All attributes will be set for the customerKey
    table.upsert {
	key customerKey
	attributes name: 'Sam', address: '456 My Way', age: 40
    }
    println table.get(customerKey)

    //get the same information via the query interface
    println table.query { keyCondition "${alias(customerId)} = ${customerKey.customerId}" } as List

    //show usage of upsert to insert a customer
    final Map upsertKey = [customerId: UUID.randomUUID()]
    table.upsert {
	key upsertKey
	attributes name: 'Benny', address: '1600 Pennsylvania Ave', age: 65
    }
    println table.get(upsertKey)

    //Now use upsert as an update
    //Use dynamo update expression to change the name and remove the age attribute
    final Map lenny = [name: 'Lenny']
    table.upsert {
	key upsertKey
	expression "set ${alias(name)} = ${lenny.name} remove ${alias(age)}"
    }
    println table.get(upsertKey)
    
    table.delete()
}

def demoMultipleKeys = { Dynamo dynamo ->
    assert !dynamo.listTables().find { name -> name == "Invoices" }
    
    //create a table with a primary key and a sort key
    dynamo.createTable("Invoices", [entry("pk", String), entry("sk", String)])

    //Change the default numeric conversion and get a table view
    //By default all numbers are left as BigDecimal because BigDecimal can handle any number
    //SMALLEST attempts to use the smallest representation for numbers
    def invoices = dynamo.newConversions(Dynamo.NumberConversion.SMALLEST).forTable("Invoices")

    //insert some data into the invoices table
    def invoiceId = UUID.randomUUID()
    invoices.put(pk: invoiceId, sk: "#d", date: '2025-11-01', terms: 'net 30', customer: 'blue man group')
    invoices.put(pk: invoiceId, sk: "#li1", quantity: 1, price: 100.25, description: 'blocks')
    invoices.put(pk: invoiceId, sk: "#li2", quantity: 20, price: 5.05, description: 'stuff')

    //get a listing of line items for that invoice, along with their prices
    //Note, queries and scans return an iterable because the dataset may be
    //very large. Iterable will automatically paginate results. If you want it
    //all at once, cast the Iterable to a List.
    def iterable = invoices.query {
	projection "${alias(sk)},${alias(price)}"
	keyCondition "${alias(pk)} = ${invoiceId} AND begins_with(${alias(sk)}, ${'#li'})"
    }
    iterable.each { println it }

    //Use the scan functionality to show every item in the invoices table
    println "doing full invoices scan"
    invoices.scan().each { println it }

    //Use filtering and projection to only retrieve certain attributes
    //where the filter criteria matches
    //This is requires the same work on the server (since all items are scanned),
    //but reduces the I/O since only some items are brought back
    println "doing filtered scan for expensive line items"
    invoices.scan {
	projection "${alias(quantity)},${alias(price)}"
	filter "begins_with(${alias(sk)}, ${'#li'}) AND ${alias(price)} > ${20}"
    }.each {
	println it
    }
}

//Fire up the local dynamo environment and then get the auto-configured client
//The Dynamo instance doesn't care where the client comes from, it just needs
//a pre-configured DynamoDbClient.
//DynamoDbClient is completely thread safe, so create as many Dynamo instances
//and as many Table views as needed and they can all safely share
//the same DynamoDbClient without issues.
try(def env = builder.inMemory()) {
    def dynamo = new Dynamo(env.client)
    demoSingleKey dynamo
    demoMultipleKeys dynamo
}
