class SmartQuery extends Aliases {
    String __expr, __projection, __index
    
    void projection(GString gstr) {
	__projection = gstr.toString()
    }
    
    void keyCondition(GString gstr) {
	__expr = __processExpression(gstr)
    }

    void index(String val) {
	__index = val
    }
}
