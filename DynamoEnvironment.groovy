@Grab('com.amazonaws:DynamoDBLocal:2.6.0')
import groovy.grape.Grape
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

class DynamoEnvironment implements AutoCloseable {
    
    static final int DEFAULT_PORT = 8_000
    static final String SYSTEM_PROPERTY = 'dynamo.local.classpath'

    final int port
    final Process proc
    final DynamoDbClient client
	
    private DynamoEnvironment(int port, Process proc) {
	this.port = port
	this.proc = proc
	this.client = DynamoDbClient.builder()
	    .region(Region.US_EAST_1)
	    .endpointOverride(new URI("http://localhost:${port}"))
	    .build();
    }
    
    void close() {
	client.close()
	proc.destroyForcibly()
    }
    
    static class Builder {
	private Integer port = DynamoEnvironment.DEFAULT_PORT
	private boolean shared = true
	private File directory = null
	private String classpath = null

	Builder port(int val) { port = val; return this; }
	Builder shared(boolean val) { shared = val; return this; }

	Builder classpath(String val) { classpath = val; return this; }

	Builder useGrapeClasspath() {
	    def uris = groovy.grape.Grape.resolve([:], [group: 'com.amazonaws', module: 'DynamoDBLocal', version: '2.6.0'])
	    def paths = uris.collect { new File(it).absolutePath }
	    return classpath(paths.join(File.pathSeparator))
	}

	Builder useSystemPropertyClasspath() {
	    return classpath(System.getProperty(SYSTEM_PROPERTY))
	}

	private List<String> getBaseCommand() {
	    def tmp = ['java', 'com.amazonaws.services.dynamodbv2.local.main.ServerRunner', '-port', port.toString()]
	    return (shared) ? tmp + ['-sharedDb'] : tmp
	}

	DynamoEnvironment inMemory() {
	    assert classpath
	    
	    def proc = new ProcessBuilder().with {
		command baseCommand + ['-inMemory']
		environment()['CLASSPATH'] = classpath
		start()
	    }

	    return new DynamoEnvironment(port, proc)
	}

	DynamoEnvironment onDisk(File directory) {
	    assert classpath

	    def proc = new ProcessBuilder().with {
		command baseCommand + ['-dbPath', directory.absolutePath]
		environment()['CLASSPATH'] = classpath
		start()
	    }

	    return new DynamoEnvironment(port, proc)
	}
    }

    static Builder builder() {
	return new Builder()
    }
}
