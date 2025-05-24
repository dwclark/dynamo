import groovy.transform.ToString
import java.util.function.Consumer
import java.util.concurrent.ConcurrentHashMap

abstract class Ops {

    private static final Map<String,String[]> parsed = new ConcurrentHashMap<>()
    
    private static class JavaGstr extends GString {
	private final String[] strs

	JavaGstr(String[] strs, Object[] args) {
	    super(args)
	    this.strs = strs
	}
	
	String[] getStrings() { strs }
    }
    
    static GString toGstr(final String template, Object[] args) {
	final String[] strs = parsed.computeIfAbsent(template, (key) -> template.split(/\{\}/))
	return new JavaGstr(strs, args)
    }
    
    interface Alias {
	String alias(String val)
    }

    interface Check extends Alias {
	void condition(GString gstr)
	void condition(String template, Object[] args)
	void key(Map<String,Object> val)
	void table(String val)
    }

    interface Query extends Alias {
	void projection(GString gstr)
	void projection(String template, Object[] args)
	void keyCondition(GString gstr)
	void keyCondition(String template, Object[] args)
	void table(String val)
	void index(String val)
    }

    interface Scan extends Alias {
	void projection(GString gstr)
	void projection(String template, Object[] args)
	void filter(GString gstr)
	void filter(String template, Object[] args)
	void index(String val)
	void table(String val)
    }
    
    interface Upsert extends Check {
	void expression(GString gstr)
	void expression(String template, Object[] args)
	void attributes(Map<String,Object> kv)
    }

    interface Delete extends Check {}

    interface Get extends Alias {
	void projection(GString gstr)
	void projection(String template, Object[] args)
	void key(Map<String,Object> val)
	void table(String name)
    }

    interface Put extends Alias {
	void condition(GString gstr)
	void condition(String template, Object[] args)
	void table(String val)
	void attributes(Map<String,Object> val)
    }
    
    interface Table {
	void put(Map<String,Object> map)
	void put(@DelegatesTo(Put) Closure config)
	void put(Consumer<Put> config)

	Map<String,Object> get(Map<String,Object> keys)
	Map<String,Object> get(@DelegatesTo(Get) Closure config)
	Map<String,Object> get(Consumer<Get> config)

	void upsert(@DelegatesTo(Upsert) Closure config)
	void upsert(Consumer<Upsert> config)
	
	Iterable<Map<String,Object>> query(@DelegatesTo(Query) Closure config)
	Iterable<Map<String,Object>> query(Consumer<Query> config)
	
	Iterable<Map<String,Object>> scan()
	Iterable<Map<String,Object>> scan(@DelegatesTo(Scan) Closure config)
	Iterable<Map<String,Object>> scan(Consumer<Scan> config)

	void delete(Map<String,Object> key)
	void delete(@DelegatesTo(Delete) Closure config)
	void delete(Consumer<Delete> config)
	void delete()

	default void putAt(Map<String,Object> keys, Map<String,Object> attrs) {
	    upsert {
		key(keys)
		attributes(attrs)
	    }
	}

	default Map<String,Object> getAt(Map<String,Object> keys) { get(keys) }
    }

    interface ReadTransaction {
	void get(Map<String,Object> keys, String table)
	void get(@DelegatesTo(Get) Closure config)
	void get(Consumer<Get> consumer)
    }

    interface WriteTransaction {
	void check(@DelegatesTo(Check) Closure config)
	void check(Consumer<Check> consumer)
	
	void put(Map<String,Object> map, String table)
	void put(@DelegatesTo(Put) Closure config)
	void put(Consumer<Put> consumer)

	void upsert(@DelegatesTo(Upsert) Closure config)
	void upsert(Consumer<Upsert> consumer)

	void delete(Map<String,Object> key, String table)
	void delete(@DelegatesTo(Delete) Closure config)
	void delete(Consumer<Delete> consumer)
    }

    @ToString(includeNames=true)
    static class All implements Alias, Check, Query, Scan, Upsert, Delete, Get, Put {
	final Map<String,Object> __params = [:]
	//why IdentityHashMap? Because we need a way to make keep aliases
	//separate from parameters. Because we freshly construct
	//an uninterned string for each alias, each alias is guaranteed to
	//be identical with no other string running in the jvm.
	//Hence, IdentityHashMap can always separate aliases from other strings
	final Map<String,String> __aliases = new IdentityHashMap()
	int __counter = 0
	String __condition
	Map<String,Object> __key
	String __table
	String __index
	String __projection
	String __keyCondition
	String __filter
	String __expression
	Map<String,Object> __attributes
	
	private String __processExpression(GString gstr) {
	    gstr.values.eachWithIndex { Object val, int index ->
		if(val !in __aliases.keySet()) {
		    final String name = ':p' + index
		    __params[name] = val
		    gstr.values[index] = name
		}
	    }
	    
	    return gstr.toString()
	}
	
	String alias(String val) {
	    final String found = __aliases.findResult { alias, v -> (v == val) ? alias : null }
	    if(found) {
		return found
	    }
	    else {
		final String alias = '#a' + (__counter++)
		__aliases[alias] = val
		return alias
	    }
	}

	void condition(GString gstr) {
	    __condition = __processExpression(gstr)
	}

	void condition(String template, Object[] args) {
	    condition(Ops.toGstr(template, args))
	}

	void key(Map<String,Object> val) {
	    __key = val
	}
	
	void table(String val) {
	    __table = val
	}
	
	void index(String val) {
	    __index = val
	}

	void projection(GString gstr) {
	    __projection = __processExpression(gstr)
	}

	void projection(String template, Object[] args) {
	    projection(Ops.toGstr(template, args))
	}

	void keyCondition(GString gstr) {
	    __keyCondition = __processExpression(gstr)
	}

	void keyCondition(String template, Object[] args) {
	    keyCondition(Ops.toGstr(template, args))
	}

	void filter(GString gstr) {
	    __filter = __processExpression(gstr)
	}
	
	void filter(String template, Object[] args) {
	    filter(Ops.toGstr(template, args))
	}
	
	void expression(GString gstr) {
	    __expression = __processExpression(gstr)
	}

	void expression(String template, Object[] args) {
	    expression(Ops.toGstr(template, args))
	}
	
	void attributes(Map<String,Object> val) {
	    __attributes = val
	}

	All convertAttributesToExpression() {
	    if(__attributes) {
		__expression = 'set ' + __attributes.collect { k, v -> "${alias(k)} = :${k}" }.join(', ')
		__attributes.each { k, v -> __params[':' + k] = v }
	    }

	    return this
	}
    }

    static All delegateAll(Closure config) {
	final All all = new All()
	config.setDelegate(all)
	config.setResolveStrategy(Closure.DELEGATE_FIRST)
	config.call()
	return all
    }

    static All noDelegate(Consumer consumer) {
	final All all = new All()
	consumer.accept(all)
	return all
    }
}
