import groovy.transform.ToString

abstract class Ops {

    interface Alias {
	String get(String missing)
	String alias(String val)
    }

    interface Check extends Alias {
	void condition(GString gstr)
	void key(Map<String,Object> val)
	void table(String val)
    }

    interface Query extends Alias {
	void projection(GString gstr)
	void keyCondition(GString gstr)
	void table(String val)
	void index(String val)
    }

    interface Scan extends Alias {
	void projection(GString gstr)
	void filter(GString gstr)
	void index(String val)
	void table(String val)
    }

    interface Upsert extends Check {
	void expression(GString gstr)
	void attributes(Map<String,Object> kv)
    }

    interface Delete extends Check {}

    interface Get extends Alias {
	void projection(GString gstr)
	void key(Map<String,Object> val)
	void table(String name)
    }

    interface Put extends Alias {
	void condition(GString gstr)
	void table(String val)
	void attributes(Map<String,Object> val)
    }
    
    interface Table {
	void put(Map<String,Object> map)
	void put(@DelegatesTo(Put) Closure config)
	Map<String,Object> get(Map<String,Object> keys)
	Map<String,Object> get(@DelegatesTo(Get) Closure config)
	void upsert(@DelegatesTo(Upsert) Closure config)
	Iterable<Map<String,Object>> query(@DelegatesTo(Query) Closure config)
	Iterable<Map<String,Object>> scan()
	Iterable<Map<String,Object>> scan(@DelegatesTo(Scan) Closure config)
	void delete(Map<String,Object> key)
	void delete(@DelegatesTo(Delete) Closure config)
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
    }

    interface WriteTransaction {
	void check(@DelegatesTo(Check) Closure config)
	void put(Map<String,Object> map, String table)
	void put(@DelegatesTo(Put) Closure config)
	void upsert(@DelegatesTo(Upsert) Closure config)
	void delete(Map<String,Object> key, String table)
	void delete(@DelegatesTo(Delete) Closure config)
    }

    @ToString(includeNames=true)
    static class All implements Alias, Check, Query, Scan, Upsert, Delete, Get, Put {
	final Map<String,Object> __params = [:]
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
	
	String get(String missing) { return missing }
	
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
	
	void keyCondition(GString gstr) {
	    __keyCondition = __processExpression(gstr)
	}

	void filter(GString gstr) {
	    __filter = __processExpression(gstr)
	}
	
	void expression(GString gstr) {
	    __expression = __processExpression(gstr)
	}
	
	void attributes(Map<String,Object> val) {
	    __attributes = val
	}

	void convertAttributesToExpression() {
	    if(__attributes) {
		__expression = 'set ' + __attributes.collect { k, v -> "${alias(k)} = :${k}" }.join(', ')
		__attributes.each { k, v -> __params[':' + k] = v }
	    }
	}
    }

    static All delegateAll(Closure config) {
	final All all = new All()
	config.setDelegate(all)
	config.setResolveStrategy(Closure.DELEGATE_FIRST)
	config.call()
	return all
    }
}
