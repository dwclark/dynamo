class Aliases {
    final Map<String,Object> __params = [:]
    final Map<String,String> __aliases = new IdentityHashMap()
    int __counter = 0
    
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
    
    protected String __processExpression(GString gstr) {
	gstr.values.eachWithIndex { Object val, int index ->
	    if(val !in __aliases.keySet()) {
		final String name = ':p' + index
		__params[name] = val
		gstr.values[index] = name
	    }
	}
	
	return gstr.toString()
    }
}
