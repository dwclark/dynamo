class SmartScan extends Aliases {
    String __projection, __filter, __index

    void projection(GString gstr) {
	__projection = gstr.toString()
    }

    void filter(GString gstr) {
	__filter = __processExpression(gstr)
    }

    void index(String val) {
	__index = val
    }
}
