//Although we have to import the local dependency to make sure
//the dynamo libraries are present, the Dynamo class fully hides
//all of the dynamo model. Because of this, the script can be
//written using nothing but groovy strings, closures, and standard
//java types such as Map, List, String, etc.
@Grab('com.amazonaws:DynamoDBLocal:2.6.0') // or at project level @Grab('software.amazon.awssdk:dynamodb:2.31.41')
@Grab('net.datafaker:datafaker:2.4.3')
import static java.util.Map.entry
import net.datafaker.Faker
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

final Faker faker = new Faker()

def demoSingleKey = { Dynamo dynamo ->
    final Map customerKey = [customerId: UUID.randomUUID().toString()]
    assert !dynamo.listTables()

    //Demo table creation with a single primary key
    final Ops.Table table = dynamo.createTable("Customers", [entry("customerId", String)])

    //put and get customer data, put is always an insert
    final Map david = customerKey + [name: 'David', address: '123 Main', age: 100]
    table.put david
    assert david == table.get(customerKey)

    //show upsert functionality, in this case since the key exists it will
    //be an update. All attributes will be set for the customerKey
    final Map samAttrs = [name: 'Sam', address: '456 My Way', age: 40]
    final Map sam = customerKey + samAttrs
    table.upsert {
	key customerKey
	attributes samAttrs
    }
    assert sam == table.get(customerKey)

    //get the same information via the query interface
    assert sam == table.query { keyCondition "${alias('customerId')} = ${customerKey.customerId}" }[0]

    //show usage of upsert to insert a customer
    final Map upsertKey = [customerId: UUID.randomUUID().toString()]
    final Map bennyAttrs = [name: 'Benny', address: '1600 Pennsylvania Ave', age: 65]
    final Map benny = upsertKey + bennyAttrs
    table.upsert {
	key upsertKey
	attributes bennyAttrs
    }
    assert benny == table.get(upsertKey)

    //Now use upsert as an update
    //Use dynamo update expression to change the name and remove the age attribute
    final Map lenny = benny - [age: 65] + [name: 'Lenny']
    table.upsert {
	key upsertKey
	expression "set ${alias('name')} = ${lenny.name} remove ${alias('age')}"
    }
    assert lenny == table.get(upsertKey)

    table.delete(customerKey)
    table.delete(upsertKey)
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
    final invoiceId = UUID.randomUUID().toString()
    final header = [pk: invoiceId, sk: "#d", date: '2025-11-01', terms: 'net 30', customer: 'blue man group']
    final li1 = [pk: invoiceId, sk: "#li1", quantity: 1, price: 100.25, description: 'blocks']
    final li2 = [pk: invoiceId, sk: "#li2", quantity: 20, price: 5.05, description: 'stuff']
    
    invoices.put header
    invoices.put li1
    invoices.put li2

    //get a listing of line items for that invoice, along with their prices
    //Note, queries and scans return an iterable because the dataset may be
    //very large. Iterable will automatically paginate results. If you want it
    //all at once, cast the Iterable to a List.
    def iterable = invoices.query { 
	projection "${alias('sk')}, ${alias('price')}, ${alias('quantity')}"
	keyCondition "${alias('pk')} = ${invoiceId} AND begins_with(${alias('sk')}, ${'#li'})"
    }

    //iterate the results, make sure numeric types have been converted to the smallest representation
    iterable.each { li ->
	assert li.quantity instanceof Integer && li.price instanceof BigDecimal
    }

    final shouldBe = [[sk: "#li1", quantity: 1, price: 100.25], [sk: "#li2", quantity: 20, price: 5.05]]
    assert shouldBe == iterable.sort { f,s -> f.sk <=> s.sk }

    //Use the scan functionality to show every item in the invoices table
    assert [header,li1,li2] == invoices.scan().sort { f,s -> f.sk <=> s.sk }

    //Use filtering and projection to only retrieve certain attributes
    //where the filter criteria matches
    //This is requires the same work on the server (since all items are scanned),
    //but reduces the I/O since only some items are brought back
    def results = invoices.scan {
	projection "${alias('quantity')},${alias('price')}"
	filter "begins_with(${alias('sk')}, ${'#li'}) AND ${alias('price')} > ${20}"
    } as List

    assert results && results.size() == 1 && results[0].price == 100.25 && results[0].quantity == 1
    invoices.delete()
}

def demoTransactions = { Dynamo dynamo ->
    final faddr = faker.address()
    final fname = faker.name()
    
    //create two tables
    //create a table with a primary key and a sort key
    final peopleTable = dynamo.createTable("People", [entry("id", String)])
    final addressesTable = dynamo.createTable("Addresses", [entry("id", String)])
    
    final personId = UUID.randomUUID().toString()
    final person = [id: personId, first: fname.firstName(), last: fname.lastName(), ssn: faker.numerify('### ## ####')]
    final addresses = (0..<5).collect {
	[id: UUID.randomUUID().toString(), personId: personId, street: faddr.streetAddress(),
	 city: faddr.city(), state: faddr.stateAbbr(), zip: faddr.zipCode()]
    }

    //Dynamo multi-table/multi-row transaction
    dynamo.writeTransaction {
	put person, "People"
	addresses.each { addr ->
	    def id = addr.id
	    def attrs = addr - [id: id]
	    upsert {
		table 'Addresses'
		key(id: id)
		attributes(attrs)
	    }
	}
    }

    //Dynamo multi-table/multi-row read transaction
    List<Map<String,Object>> all = dynamo.readTransaction {
	get 'People', id: personId
	addresses.each { addr -> get 'Addresses', id: addr.id }
    }
    
    assert all.size() == 6
    assert all.collect { it.id } as Set == ([person] + addresses).collect { it.id } as Set

    //Delete first two addresses we created, but only if the person has a particular ssn
    dynamo.writeTransaction {
	check {
	    table "People"
	    key id: personId
	    condition "${alias('ssn')} = ${person.ssn}"
	}
	
	delete 'Addresses', id: addresses[0].id
	delete 'Addresses', id: addresses[1].id
    }
    
    List<Map<String,Object>> reduced = dynamo.readTransaction {
	addresses.each { addr -> get 'Addresses', id: addr.id }
    }
    
    //returns empty maps when id is not found, remove empty maps for count
    assert reduced.findAll { it }.size() == 3
    
    peopleTable.delete()
    addressesTable.delete()
}

def demoAt = { Dynamo dynamo ->
    final fname = faker.name()
    final Ops.Table accounts = dynamo.createTable("Accounts", [entry("id", Number)])
    (1..10).each { id ->
	accounts[[id: id]] = [firstName: fname.firstName(), lastName: fname.lastName(), balance: id * 100]
    }

    (1..10).each { id ->
	assert accounts[[id: id]].balance == id * 100
    }
}

def demoExactlyOnce = { Dynamo dynamo ->
    final Ops.Table events = dynamo.createTable("Events", [entry("id", String)])
    final Ops.Table customers = dynamo.createTable("Customers", [entry("id", String)])
    
    final String customerId = UUID.randomUUID().toString()
    final List eventIds = (0..1).collect { UUID.randomUUID().toString() }
    final List toProcess = [[eventId: eventIds[0], customerId: customerId, first: 'Scooby', last: 'Doo', type: 'add'],
			    [eventId: eventIds[1], customerId: customerId, first: 'Scrappy', last: 'Doo', type: 'change'],
			    [eventId: eventIds[0], customerId: customerId, first: 'Scooby', last: 'Doo', type: 'add']]

    def save = { event ->
	dynamo.writeTransaction {
	    upsert {
		table 'Customers'
		key id: event.customerId
		attributes event.subMap('first', 'last')
	    }

	    put {
		table 'Events'
		attributes id: event.eventId, type: event.type
		condition "attribute_not_exists(${alias('id')})"
	    }
	}
    }

    save(toProcess[0])
    save(toProcess[1])
    
    try {
	save(toProcess[2])
	assert false
    }
    catch(software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException tce) {
	assert true
    }
	

    assert events.scan().size() == 2
    assert customers.scan().size() == 1
    assert customers[[id: customerId]].first == 'Scrappy'
}

//Manages a local dynamo environment. If you are hitting an actual Dynamo instance
//in AWS, you would just configure the client directly
def builder = DynamoEnvironment.builder().tap {
    port 12500
    shared false
    useGrapeClasspath()
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
    demoTransactions dynamo
    demoAt dynamo
    demoExactlyOnce dynamo
}
