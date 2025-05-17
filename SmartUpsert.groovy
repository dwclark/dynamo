class SmartUpsert extends Aliases {
    Map<String,Object> __key
    void key(Map<String,Object> val) { __key = val }
    
    String __updateExpression
    
    void expression(GString gstr) {
	__updateExpression = __processExpression(gstr)
    }
    
    void attributes(Map<String,Object> kv) {
	__updateExpression = 'set ' + kv.collect { k, v -> "${alias(k)} = :${k}" }.join(', ')
	kv.each { k, v -> __params[':' + k] = v }
    }
}
