@Grab('com.amazonaws:DynamoDBLocal:2.6.0') // or at project level @Grab('software.amazon.awssdk:dynamodb:2.31.41')
import software.amazon.awssdk.core.waiters.WaiterResponse
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter
import static java.util.Map.entry

def builder = DynamoEnvironment.builder().tap {
    port 12500
    shared false
    useGrapeClasspath()
}

try(def env = builder.inMemory()) {
    def dynamo = new Dynamo(env.client)

    final customerKey = [customerId: UUID.randomUUID()]
    assert [] == dynamo.listTables()
    dynamo.createTable("Customers", [entry("customerId", String)])

    def table = dynamo.forTable("Customers")
    table.put(customerKey + [name: 'David', address: '123 Main', age: 100])
    println table.get(customerKey)
    table.upsert {
	key customerKey
	attributes name: 'Sam', address: '456 My Way', age: 40
    }
    println table.get(customerKey)
    println table.query { keyCondition "customerId = ${customerKey.customerId}" } as List

    final withUpsert = [customerId: UUID.randomUUID()]
    def person = [name: 'Benny', address: '1600 Pennsylvania Ave', age: 65]
    table.upsert {
	key withUpsert
	expression "set ${alias(name)} = ${person.name}, ${alias(address)} = ${person.address}, ${alias(age)} = ${person.age}"
    }

    println table.get(withUpsert)
    person = [name: 'Obama', address: '1600 Pennsylvania Ave', age: 65]
    table.upsert {
	key withUpsert
	attributes person
    }
    
    println table.get(withUpsert)
    table.delete()

    //now try with sort keys
    dynamo.createTable("Invoices", [entry("pk", String), entry("sk", String)])
    def invoices = dynamo.newConversions(Dynamo.NumberConversion.SMALLEST).forTable("Invoices")
    def invoiceId = UUID.randomUUID()
    invoices.put(pk: invoiceId, sk: "#d", date: '2025-11-01', terms: 'net 30', customer: 'blue man group')
    invoices.put(pk: invoiceId, sk: "#li1", quantity: 1, price: 100.25, description: 'blocks')
    invoices.put(pk: invoiceId, sk: "#li2", quantity: 20, price: 5.05, description: 'stuff')
    def iterable = invoices.query {
	projection "${alias(sk)},${alias(price)}"
	keyCondition "${alias(pk)} = ${invoiceId} AND begins_with(${alias(sk)}, ${'#li'})"
    }
    iterable.each { println it }

    println "doing full invoices scan"
    invoices.scan().each { println it }
	
    println "doing filtered scan for expensive line items"

    invoices.scan {
	projection "${alias(quantity)},${alias(price)}"
	filter "begins_with(${alias(sk)}, ${'#li'}) AND ${alias(price)} > ${20}"
    }.each {
	println it
    }

}
