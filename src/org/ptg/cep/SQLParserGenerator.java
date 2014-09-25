/*
Licensed under gpl.
Copyright (c) 2010 sumit singh
http://www.gnu.org/licenses/gpl.html
Use at your own risk
Other licenses may apply please refer to individual source files.
 */

package org.ptg.cep;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import Zql.ZInsert;
import Zql.ZQuery;
import Zql.ZSelectItem;
import Zql.ZStatement;
import Zql.ZUpdate;
import Zql.ZqlParser;

public class SQLParserGenerator {
	private static final String MSG_Header = "\n/***********************************/\n";
	private Stack<String> stk = new Stack<String>();
	private Map<String, List<String>> depends = new TreeMap<String, List<String>>();
	private Map<String, ZFromItem> frms = new TreeMap<String, ZFromItem>();
	private Map<String, String> aliasTables = new TreeMap<String, String>();
	private Map<String, String> reverseAlias = new TreeMap<String, String>();
	private Map<String, Pair> results = new TreeMap<String, Pair>();
	private Map<Integer, String> codes = new TreeMap<Integer, String>();
	private static Map<String, String> dependTable = new TreeMap<String, String>();
	private static Map<String, Integer> dependParamTable = new TreeMap<String, Integer>();
	private static Map<String, String> clzReg = new TreeMap<String, String>();
	private static Map<String, String> reverseclzReg = new TreeMap<String, String>();
	static {
		dependTable.put("within", "this");
		dependTable.put("after", "this");
		dependTable.put("mysum", "this");
		dependTable.put("groupBy", "this");
		dependTable.put("timeDelta", "this");
		dependTable.put("updateTime", "this");
		dependTable.put("gt", "this");
		dependTable.put("lt", "this");
		dependTable.put("eq", "this");
		dependTable.put("isnull", "this");
		dependTable.put("notnull", "this");
		dependTable.put("stdetect", "this");
		dependTable.put("tdetect", "this");
		dependTable.put("sdetect", "this");
		dependTable.put("eval", "this");
		dependTable.put("sendError", "this");
		dependTable.put("sendForward", "this");
		dependTable.put("sendCtrlZ", "this");
		dependTable.put("sendCopy", "this");
		dependTable.put("sendCopyAsync", "this");
		dependTable.put("sendErrorAsync", "this");
		dependTable.put("sendForwardAsync", "this");
		dependTable.put("sendCtrlZAsync", "this");
		dependTable.put("getMatches", "this");
		dependTable.put("jsonpath", "this");
		dependTable.put("mySqlString", "this");

		dependTable.put("applyHint", "this");
		dependTable.put("readHint", "this");

		dependTable.put("mysd", "this");
		dependTable.put("mymean", "this");
		dependTable.put("mymax", "this");
		dependTable.put("mymin", "this");
		dependTable.put("mygm", "this");
		dependTable.put("myncount", "this");
		dependTable.put("mypercentile", "this");
		dependTable.put("myskew", "this");
		dependTable.put("mysumSq", "this");
		dependTable.put("myvar", "this");
		dependTable.put("mywsize", "this");

		dependTable.put("mygetFreq", "this");
		dependTable.put("mygetCumFreq", "this");
		dependTable.put("mygetCumPct", "this");
		dependTable.put("myintercept", "this");

		dependTable.put("myiStdErr", "this");
		dependTable.put("mymeanSqErr", "this");
		dependTable.put("myN", "this");
		dependTable.put("myR", "this");
		dependTable.put("myregSumSq", "this");
		dependTable.put("myrSq", "this");
		dependTable.put("mysig", "this");
		dependTable.put("myslope", "this");

		dependTable.put("myslopeCI", "this");
		dependTable.put("myslopeCIVal", "this");
		dependTable.put("myslopeStdErr", "this");
		dependTable.put("mysumCrossProd", "this");

		dependTable.put("mysumSqErrors", "this");
		dependTable.put("mytotalSumSq", "this");
		dependTable.put("myxSumSq", "this");

		dependTable.put("jxpath", "this");
		dependTable.put("jexl", "this");
		dependTable.put("script", "this");

		dependParamTable.put("mysd", 2);
		dependParamTable.put("mymean", 2);
		dependParamTable.put("mymax", 2);
		dependParamTable.put("mymin", 2);
		dependParamTable.put("mygm", 2);
		dependParamTable.put("myncount", 2);
		dependParamTable.put("mypercentile", 2);
		dependParamTable.put("myskew", 2);
		dependParamTable.put("mysumSq", 2);
		dependParamTable.put("myvar", 2);
		dependParamTable.put("mywsize", 2);
		dependParamTable.put("mygetFreq", 2);
		dependParamTable.put("mygetCumFreq", 2);
		dependParamTable.put("mygetCumPct", 2);
		dependParamTable.put("myintercept", 1);
		dependParamTable.put("myiStdErr", 1);
		dependParamTable.put("mymeanSqErr", 1);
		dependParamTable.put("myN", 1);
		dependParamTable.put("myR", 1);
		dependParamTable.put("myregSumSq", 1);
		dependParamTable.put("myrSq", 1);
		dependParamTable.put("mysig", 1);
		dependParamTable.put("myslope", 1);
		dependParamTable.put("myslopeCI", 1);
		dependParamTable.put("myslopeCIVal", 1);
		dependParamTable.put("myslopeStdErr", 1);
		dependParamTable.put("mysumCrossProd", 1);
		dependParamTable.put("mysumSqErrors", 1);
		dependParamTable.put("mytotalSumSq", 1);
		dependParamTable.put("myxSumSq", 1);
		dependParamTable.put("readHint", 0);
		dependParamTable.put("applyHint", 1);
		dependParamTable.put("updateTime", 0);
		dependParamTable.put("mySqlString", 1);
		dependParamTable.put("getMatches", 2);
		dependParamTable.put("sendError", 2);
		dependParamTable.put("sendForward", 2);
		dependParamTable.put("sendCtrlZ", 2);
		dependParamTable.put("sendCopy", 2);
		dependParamTable.put("sendCopyAsync", 2);
		dependParamTable.put("sendErrorAsync", 2);
		dependParamTable.put("sendForwardAsync", 2);
		dependParamTable.put("sendCtrlZAsync", 2);
		dependParamTable.put("timeDelta", 1);
		dependParamTable.put("within", 2);
		dependParamTable.put("after", 2);
		dependParamTable.put("mysum", 2);
		dependParamTable.put("groupBy", 3);
		dependParamTable.put("sdetect", 1);
		dependParamTable.put("tdetect", 1);
		dependParamTable.put("stdetect", 2);
		dependParamTable.put("gt", 2);
		dependParamTable.put("lt", 2);
		dependParamTable.put("eq", 2);
		dependParamTable.put("notnull", 1);
		dependParamTable.put("isnull", 1);
		dependParamTable.put("eval", 1);
		dependParamTable.put("jsonpath", 2);
		dependParamTable.put("jxpath", 2);
		dependParamTable.put("jexl", 2);
		dependParamTable.put("script", 3);

		clzReg.put("tickerevent", "cluster.TickerEvent");
		clzReg.put("queryevent", "cluster.query.QueryEvent");
		clzReg.put("event", "org.ptg.events.Event");
		clzReg.put("Event", "org.ptg.events.Event");
		clzReg.put("Object", "java.lang.Object");

		clzReg.put("File", "java.io.File");
		clzReg.put("Date", "java.util.Date");
		clzReg.put("RS", "java.sql.ResultSet");
		clzReg.put("RS", "java.sql.ResultSet");
		clzReg.put("RS", "java.sql.ResultSet");

		clzReg.put("Method", "java.lang.reflect.Method");
		clzReg.put("Field", "java.lang.reflect.Field");

	}
	private static Map<Integer, String> strNum = new HashMap<Integer, String>();
	static {
		strNum.put(0, "zero");
		strNum.put(1, "one");
		strNum.put(2, "two");
		strNum.put(3, "three");
		strNum.put(4, "four");
		strNum.put(5, "five");
		strNum.put(6, "six");
		strNum.put(7, "seven");
		strNum.put(8, "eight");
		strNum.put(9, "nine");
		strNum.put(10, "ten");
		strNum.put(11, "eleven");
		strNum.put(12, "twelve");
	}

	public void clear() {
		stk.clear();
		depends.clear();
		frms.clear();
		results.clear();
		codes.clear();
	}

	public void addUDF(String alias, String actual) {
		dependTable.put(alias, actual);
	}

	public void addEventMapping(String alias, String actual) {
		clzReg.put(alias, actual);
		reverseclzReg.put(actual, alias);
	}

	public void prepareParser(ZqlParser p) {
		for (String s : dependTable.keySet()) {
			p.addCustomFunction(s, dependParamTable.get(s));
		}
	}

	public String runQuery(String str) {
		clear();
		String retstr = "";
		try {
			ZqlParser p = null;
			str = preprocess(str);
			// System.out.println(str);
			p = new ZqlParser(new ByteArrayInputStream(str.getBytes()));
			prepareParser(p);
			ZStatement st;
			st = p.readStatement();
			// System.out.println(st.toString());
			if (st instanceof ZQuery) {
				retstr = buildQuery((ZQuery) st);
			} else if (st instanceof ZInsert) {
				buildInsert((ZInsert) st);
			} else if (st instanceof ZUpdate) {
				buildUpdate((ZUpdate) st);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		// System.out.println(stk);
		return retstr;

	}

	public String runQueryNoPreprocess(String str) {
		clear();
		String retstr = "";
		try {
			ZqlParser p = null;
			// System.out.println(str);
			p = new ZqlParser(new ByteArrayInputStream(str.getBytes()));
			prepareParser(p);
			ZStatement st;
			st = p.readStatement();
			// System.out.println(st.toString());
			if (st instanceof ZQuery) {
				retstr = buildQuery((ZQuery) st);
			} else if (st instanceof ZInsert) {
				buildInsert((ZInsert) st);
			} else if (st instanceof ZUpdate) {
				buildUpdate((ZUpdate) st);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		// System.out.println(stk);
		return retstr;

	}

	public String buildQuery(ZQuery q) {
		String toret = "";

		// System.out.println("From: " + q.getFrom());
		Vector<ZFromItem> froms = q.getFrom();

		for (ZFromItem f : froms) {
			// System.out.println("From:+\t" + (f.getTable()));
			String temp = f.getAlias() == null ? f.getTable() : f.getAlias();
			frms.put(temp, f);
			aliasTables.put(f.getAlias(), f.getTable());
			reverseAlias.put(f.getTable(), f.getAlias());
		}

		// System.out.println("Select: " + q.getSelect());
		Vector<ZSelectItem> sels = q.getSelect();
		for (ZSelectItem i : sels) {
			String res = i.getAlias() == null ? i.getColumn() : i.getAlias();
			System.out.println("Selecting:" + res);
			results.put(res, new Pair(i.getTable(), i.getColumn()));
		}

		// System.out.println("Query: " + q.getWhere());
		ZExp z = q.getWhere();
		String s = null;
		if (z instanceof ZConstant) {
			ZConstant zc = (ZConstant) z;
			// System.out.println("Constant where clause (not supported): " +
			// zc.getValue());
		} else if (z instanceof ZExpression) {
			ZExpression ze = (ZExpression) z;
			toret = processsRule(ze, 1, frms);
		} else if (z instanceof ZQuery) {
			// System.out.println("Inner queries not supported: " + z);
		}
		addFromDependency();
		return toret;
	}

	public Map<String, List<String>> getDepends() {
		return depends;
	}

	public void setDepends(Map<String, List<String>> depends) {
		this.depends = depends;
	}

	public String getFireFunction(String s) {
		String temp = "{";
		temp += getTableDependencyTypeCast();
		if (s == null || s.length() < 1) {
			s = "true";
		}
		temp += "if (" + s + "){return true;} else{return false;}}";
		return temp;
	}

	public String getJoinFireFunction(String s) {
		String temp = "{";
		String joincol = "";
		temp += getTableDependencyTypeCast();
		if (s == null || s.length() < 1) {
			s = "true";
		}
		temp += "{";
		temp += "prepareForJoin();\n";
		temp += addItemToCollection();
		boolean first = true;
		for (String key : frms.keySet()) {
			ZFromItem f = frms.get(key);
			temp += " java.util.Collection col" + f.getTable() + " = new java.util.ArrayList();\n";
			if (first) {
				first = false;
			} else {
			}
		}
		List<String> keys = new ArrayList<String>();
		keys.addAll(frms.keySet());
		String[] keyarr = keys.toArray(new String[0]);
		CollectionUtils.reverseArray(keys.toArray(keyarr));
		first = true;
		for (String key : keyarr) {
			ZFromItem f = frms.get(key);
			if (first) {
				joincol += "col" + f.getTable();
				first = false;
			} else {
				joincol += "," + "col" + f.getTable();
			}
		}
		temp += getJoinDepTypeCast();
		temp += "fullCartesian(new java.util.Collection[]{" + joincol + "});";
		temp += "}";

		temp += "}";
		return temp;
	}

	public String getDeltaJoinFireFunction(String s) {
		String temp = "{";
		String joincol = "";
		temp += getTableDependencyTypeCast();
		if (s == null || s.length() < 1) {
			s = "true";
		}
		temp += "{";
		temp += "prepareForJoin();\n";
		temp += addItemToCollection();
		boolean first = true;
		for (String key : frms.keySet()) {
			ZFromItem f = frms.get(key);
			temp += " java.util.Collection col" + f.getTable() + " = new java.util.ArrayList();\n";
			if (first) {
				first = false;
			} else {
			}
		}
		List<String> keys = new ArrayList<String>();
		keys.addAll(frms.keySet());
		String[] keyarr = keys.toArray(new String[0]);
		CollectionUtils.reverseArray(keys.toArray(keyarr));
		first = true;
		for (String key : keyarr) {
			ZFromItem f = frms.get(key);
			if (first) {
				joincol += "col" + f.getTable();
				first = false;
			} else {
				joincol += "," + "col" + f.getTable();
			}
		}
		temp += getJoinDepTypeCast();
		temp += "deltaCartesian(new java.util.Collection[]{" + joincol + "});";
		temp += "}";

		temp += "}";
		return temp;
	}

	public String getJoinDepTypeCast() {
		String str = "";
		boolean first = true;
		int i = 1;
		for (String s : frms.keySet()) {
			if (s.length() > 0) {
				ZFromItem f = frms.get(s);
				if (first) {
					str += "col" + f.getTable() + ".add( $1[" + (i - 1) + "]);";
					first = false;
				} else {
					str += "col" + f.getTable() + ".add( $1[" + (i - 1) + "]);";
				}
			}
			i++;
		}
		return str;
	}

	public String getDependencies() {
		String temp = "";
		temp += "{java.util.List rets = new java.util.ArrayList();";
		for (String str : depends.keySet()) {
			temp += "rets.add(\"" + (clzReg.get(str) == null ? str : clzReg.get(str)) + "\");";
		}
		temp += "return rets;}";
		return temp;
	}

	public String isDependent() {
		String temp = "{";
		// temp+=( "public boolean isDepends(java.lang.String s){");
		for (String str : depends.keySet()) {
			temp += "if(\"" + (clzReg.get(str) == null ? str : clzReg.get(str)) + "\".equals($1))return true;";
		}
		temp += "return false;}";
		return temp;
	}

	public String isColDependent() {
		String temp = "{";
		// temp+=(
		// "public boolean isDepends(java.lang.String s,java.lang.String s2){");
		for (String str : depends.keySet()) {
			temp += "if(\"" + (clzReg.get(str) == null ? str : clzReg.get(str)) + "\".equals($1)){";
			for (String str2 : depends.get(str)) {
				temp += "if(\"" + str2 + "\".equals($2))return true;";
			}
			temp += "}";
		}
		temp += "return false;}";
		return temp;
	}

	public String getConditionedTable(String table) {
		return StringUtils.replace(clzReg.get(table) == null ? table : clzReg.get(table), ".", "_");
	}

	public String getColumnDependencies(String table) {
		String temp = "{";
		// temp+=(
		// "public java.util.List<String> getDepends"+getConditionedTable(table)+"(){");
		temp += "java.util.List rets = new java.util.ArrayList();";
		for (String str : depends.get(table)) {
			temp += "rets.add(\"" + str + "\");";
		}
		temp += "return rets;}";
		return temp;
	}

	public String processsRule(ZExpression ze, int depth, Map<String, ZFromItem> frms) {
		String str = "";
		push(ze.getOperator());
		// System.out.println(getTabStr(depth) + "Depth: [" + depth + "] " +
		// "Rule Type:" + ze.getOperator());
		if (dependTable.containsKey(ze.getOperator())) {
			str = handleCustom(ze, depth, frms);
		} else {
			str = handleNormal(ze, depth, frms);
		}

		return str;
	}

	public String handleNormal(ZExpression ze, int depth, Map<String, ZFromItem> frms) {
		// System.out.println("Normal operator");
		String str = "";
		boolean second = false;
		Vector<ZExp> operands = ze.getOperands();
		Iterator<ZExp> opi = operands.iterator();
		ZExp op = null;
		if (opi.hasNext()) {
			op = opi.next();
		}
		MOVENEXT: while (op != null) {
			if (op instanceof ZExpression) {
				ZExpression zexpart = (ZExpression) op;
				if (second) {
					str += getCondOperator(ze.getOperator());
				} else if (!second && !ze.getOperator().equalsIgnoreCase("and") && !ze.getOperator().equalsIgnoreCase("or") && !ze.getOperator().equalsIgnoreCase("&&")
						&& !ze.getOperator().equalsIgnoreCase("||")) {
					{
						str += getCondOperator(ze.getOperator());
					}
				}
				str += "(";
				str += processsRule(zexpart, depth + 1, frms);
				str += ")";
				push(str);
				second = true;
			} else if (op instanceof ZConstant) {
				ZConstant zc = (ZConstant) op;
				if (opi.hasNext()) {
					op = opi.next();
				} else {
					op = null;
				}
				if (op instanceof ZConstant) {
					ZConstant zc2 = (ZConstant) op;
					str += printMatchExpr(zc, zc2, depth, ze.getOperator(), frms);
				} else {
					str += printSingleMatchExpr(depth, ze.getOperator(), frms, zc);
					continue MOVENEXT;
				}
			} else if (op instanceof ZQuery) {
				ZQuery zq = (ZQuery) op;
				handleSubQuery(zq);
			}
			if (opi.hasNext()) {
				op = opi.next();
			} else {
				op = null;
			}
		}
		return str;
	}

	public String handleCustom(ZExpression ze, int depth, Map<String, ZFromItem> frms) {
		// System.out.println("Custom operator");
		String str = "";
		boolean second = false;
		Vector<ZExp> operands = ze.getOperands();
		Iterator<ZExp> opi = operands.iterator();
		ZExp op = null;
		List<ZConstant> params = new ArrayList<ZConstant>();
		while (opi.hasNext()) {
			op = opi.next();
			if (op instanceof ZConstant) {
				ZConstant zc = (ZConstant) op;
				params.add(zc);
			} else {
				throw new IllegalArgumentException("Parameter cannot be anything else then ZCOnstant");
			}
		}
		pop();
		return "(" + printCustomMatchExpr(depth, ze.getOperator(), frms, params.toArray(new ZConstant[0])) + ")";
	}

	public void handleSubQuery(ZQuery zq) {
		// System.out.println("Inner queries not supported: " + zq);

	}

	public void handleInnerQuery(ZQuery zq) {
		// System.out.println("Inner queries not supported in operand parts: " +
		// zq);
	}

	public void buildUpdate(ZUpdate q) {
		// System.out.println("Not supported (Yet ;-) )");
	}

	public void buildInsert(ZInsert q) {
		// System.out.println("Not supported ( Yet ;-) )");
	}

	public String getTabStr(int depth) {
		String ret = "\t";
		for (int i = 0; i < depth; i++) {
			ret += "\t";
		}
		return ret;
	}

	public String printMatchExpr(ZConstant zc, ZConstant zc2, int depth, String operator, Map<String, ZFromItem> frms) {
		// System.out.println(getTabStr(depth) + "Depth: [" + depth + "] " +
		// "Operand: " + zc.getValue() + ",type: " + zc.getType());
		// System.out.println(getTabStr(depth) + "Depth: [" + depth + "] " +
		// "Operand: " + zc2.getValue() + ",type: " + zc2.getType());
		ZConstant lexpr = null, rexpr = null;
		if (zc.getType() == 0) {
			rexpr = zc2;
			lexpr = zc;
		} else if (zc2.getType() == 0) {
			rexpr = zc2;
			lexpr = zc;
		} else {
			rexpr = zc2;
			lexpr = zc;
		}
		String s = getStr(lexpr, rexpr, frms, operator);
		// System.out.println("Returning (" + s + ")");
		return s;
	}

	public String printCustomMatchExpr(int depth, String operator, Map<String, ZFromItem> frms, ZConstant... zc) {
		// System.out.println(getTabStr(depth) + "Custom, Depth: [" + depth +
		// "] " + "Operand: " + operator + ",type: " + zc);
		String s = "";

		boolean first = true;
		for (ZConstant i : zc) {
			String stemp = i.getValue();
			if (first) {
				if (i.getType() == 0) {
					stemp = getGetter(stemp);

				}
				s += stemp;
				first = false;
			} else {
				if (i.getType() == 0) {
					stemp = getGetter(stemp);
				}
				s += "," + stemp;
			}
			if (i.getType() == 0) {
				addDependency(i.getValue(), false);
			}
		}

		// System.out.println(s);
		String deps = dependTable.get(operator);
		if (deps != null) {
			addDependency(deps, operator, true);
		}
		// System.out.println("Returning ( " + deps + "." + operator + "(" + s +
		// ")" + " )");
		return deps + "." + operator + "(" + s + ")";

	}

	public String printSingleMatchExpr(int depth, String operator, Map<String, ZFromItem> frms, ZConstant zc) {
		// System.out.println(getTabStr(depth) + "Depth: [" + depth + "] " +
		// "Operand: " + zc.getValue() + ",type: " + zc.getType());
		if (zc.getType() == 3) {
			String s = zc.getValue();
			// System.out.println(s);
			String deps = dependTable.get(s);
			if (deps != null) {
				addDependency(deps, s, true);
			}
			// System.out.println("Returning " + getGetter(s));
			return getGetter(s);
		} else {
			return "";
		}
	}

	public String getStr(ZConstant lx, ZConstant rx, Map<String, ZFromItem> frms, String op) {
		/*
		 * if (op.equalsIgnoreCase("LIKE")) {// reverse order ZConstant temp =
		 * rx; rx = lx; lx = temp; }
		 */op = getCondOperator(op);
		String s1, s2;
		s1 = getValExtractor(lx, frms, op);
		s2 = getValExtractor(rx, frms, op);
		if (op.equalsIgnoreCase("==") && rx.getType() == 3) {
			op = ".equals";
		}
		return s1 + op + "(" + s2 + ")";
	}

	public String getValExtractor(ZConstant zc, Map<String, ZFromItem> frms, String operator) {

		if (zc.getType() == 1) {
			return "null";
		} else if (zc.getType() == 2) {
			return zc.getValue();
		} else if (zc.getType() == 3) {
			return "\"" + zc.getValue() + "\"";
		} else if (zc.getType() == 0) {
			/* add dependencies */
			String[] parts = zc.getValue().split("\\.");
			ZFromItem zfrm = frms.get(parts[0]);
			addDependency(zfrm.getTable(), parts[1], false);
			/* end add dependencies */
			String tbl = reverseAlias.get(zfrm.getTable()) == null ? parts[0] : reverseAlias.get(zfrm.getTable());
			return tbl + getGetter(parts[1]);
		} else {
			return null;
		}

	}

	public String getCondOperator(String op) {
		if (op.equalsIgnoreCase("LIKE")) {// reverse order
			return ".matches";
		}
		if (op.equalsIgnoreCase("=")) {// reverse order
			return "==";
		}
		if (op.equalsIgnoreCase("AND")) {// reverse order
			return "&&";
		}
		if (op.equalsIgnoreCase("OR")) {// reverse order
			return "||";
		}
		if (op.equalsIgnoreCase("NOT")) {// reverse order
			return "!";
		}
		return op;
	}

	public void push(String s) {
		// System.out.println("Pushing on stack:>>> " + s);
		stk.push(s);
	}

	public void push() {
		// System.out.println("Pushing on stack(EMPTY STRING):>>> " + "");
		stk.push("");
	}

	public String pop() {
		String s = stk.pop();
		// System.out.println("Popped fromstack:<<< " + s);
		return s;
	}

	private void addDependency(String from, String what, boolean system) {
		if (from.equals("this")) {
			return;// this is already there.
		}
		String apd = system ? "SYS" : "";
		List l = depends.get(from);
		if (l == null) {
			String s = aliasTables.get(from);
			if (s != null) {
				l = depends.get(s);
			}
			if (l == null) {
				l = new ArrayList<String>();
			}
			l.add(what);
			// System.out.println("Putting : " + apd + "\t\t>>" + from + "-->" +
			// what);
			depends.put(from, l);

		} else {
			l.add(what);
			// System.out.println("Putting: " + apd + "\t\t>>" + from + "-->" +
			// what);
			depends.put(from, l);
		}
	}

	private void addDependency(String ele, boolean system) {
		if (ele.equals("this")) {
			return;// this is already there.
		}
		String apd = system ? "SYS" : "";
		String[] parts = ele.split("\\.");
		if (parts.length < 2) {
			List l = depends.get(ele);
			if (l == null) {
				String s = aliasTables.get(parts[0]);
				if (s != null) {
					l = depends.get(s);
				}
				if (l == null) {
					l = new ArrayList<String>();
				}
				ZFromItem tempstr = frms.get(ele);
				String tstr = tempstr.getTable();
				// tstr = clzReg.get(tstr);
				// System.out.println("Putting: " + tstr);
				depends.put(tstr, l);
			}
			// System.out .println("Please fully qualify variable to be used" +
			// ele);
			return;
		}
		parts[1] = fixGetter(ele);
		List l = depends.get(parts[0]);
		if (l == null) {
			String s = aliasTables.get(parts[0]);
			if (s != null) {
				l = depends.get(s);
			}
			if (l == null) {
				l = new ArrayList<String>();
			}
			l.add(parts[1]);
			// System.out.println("Putting: " + apd + "\t\t>>" + parts[0] +
			// "-->" + parts[1]);
			depends.put(s == null ? parts[0] : s, l);
		} else {
			l.add(parts[1]);
			// System.out.println(apd + "\t\t>>" + parts[0] + "-->" + parts[1]);
			depends.put(parts[0], l);
		}
	}

	public void addFromDependency() {
		for (ZFromItem tempstr : frms.values()) {
			String tstr = tempstr.getTable();
			List l = depends.get(tstr);
			if (l == null) {
				l = new ArrayList<String>();
			}
			l.add(tstr);
			depends.put(tstr, l);
		}
	}

	public String getGetter(String s) {
		String[] parts = new String[2];
		parts[0] = StringUtils.substringBeforeLast(s, ".");
		parts[1] = StringUtils.substringAfterLast(s, ".");
		if (s.contains(".")) {
			if (parts[1].indexOf("get") == -1) {
				String tbl = reverseAlias.get(parts[0]) == null ? parts[0] : reverseAlias.get(parts[0]);
				if (frms.containsKey(parts[0]) == false) {
					if (StringUtils.contains(s, '(')) {
						return StringUtils.replace(s, "\'", "\"");
					} else {
						return tbl + "." + parts[1] + "()";
					}
				} else {
					return tbl + ".get" + WordUtils.capitalize(parts[1]) + "()";
				}
			} else {
				return s + "()";
			}
		} else {
			if (s.indexOf("get") == -1) {
				if (frms.containsKey(s)) {
					return s;
				} else {
					return ".get" + WordUtils.capitalize(s) + "()";
				}
			}
		}

		return "." + s + "()";
	}

	public String fixGetter(String s) {
		String[] parts = s.split("\\.");
		if (parts.length == 2) {
			if (parts[1].indexOf("get") == -1) {
				return parts[1];
			} else {
				return WordUtils.uncapitalize(parts[1].substring(3, parts[1].length()));
			}
		} else {
			if (s.indexOf("get") == -1) {
				return s;
			} else {
				return WordUtils.uncapitalize(s.substring(s.indexOf("get"), s.length()));
			}
		}
	}

	public String getTableDependencyString() {
		// String str = "org.ptg.events.Event []evt";
		String str = "Object []evt";
		return str;
	}

	public String getTableDependencyTypeCast() {
		String str = "";
		boolean first = true;
		int i = 1;
		for (String s : frms.keySet()) {
			if (s.length() > 0) {
				ZFromItem f = frms.get(s);
				if (first) {
					str += clzReg.get(f.getTable()) + " " + (f.getAlias() == null ? "_" + f.getTable() : f.getAlias()) + " = ( " + clzReg.get(f.getTable()) + " )$1[" + (i - 1) + "];\n";
					first = false;
				} else {
					str += clzReg.get(f.getTable()) + " " + (f.getAlias() == null ? "_" + f.getTable() : f.getAlias()) + " = ( " + clzReg.get(f.getTable()) + " ) $1[" + (i - 1) + "];\n";
				}
			}
			i++;
		}
		return str;
	}

	public String addItemToCollection() {
		String str = "";
		boolean first = true;
		int i = 1;
		for (String s : frms.keySet()) {
			if (s.length() > 0) {
				ZFromItem f = frms.get(s);
				if (first) {
					String tempVar = f.getAlias() == null ? "_" + f.getTable() : f.getAlias();
					str += "\n if ( " + tempVar + "!=null||nullAllowed()){\n";
					str += "addJoinItem( \"" + clzReg.get(f.getTable()) + "\", " + tempVar + " );\n ";
					str += "}\n";
					first = false;
				} else {
					String tempVar = f.getAlias() == null ? "_" + f.getTable() : f.getAlias();
					str += "\n if ( " + tempVar + "!=null ||nullAllowed()){\n";
					str += "addJoinItem( \"" + clzReg.get(f.getTable()) + "\", " + tempVar + " );\n ";
					str += "}\n";
				}
			}
			i++;
		}
		return str;
	}

	public String getTableDepTypeCastNoArray() {
		String str = "";
		boolean first = true;
		int i = 1;
		for (String s : frms.keySet()) {
			if (s.length() > 0) {
				ZFromItem f = frms.get(s);
				if (first) {
					str += clzReg.get(f.getTable()) + " " + (f.getAlias() == null ? "_" + f.getTable() : f.getAlias()) + " = ( " + clzReg.get(f.getTable()) + " )$" + i + ";";
					first = false;
				} else {
					str += clzReg.get(f.getTable()) + " " + (f.getAlias() == null ? "_" + f.getTable() : f.getAlias()) + " = ( " + clzReg.get(f.getTable()) + " ) $" + i + ";";
				}
			}
			i++;
		}
		return str;
	}

	public String getTableDepStrNonArray() {
		String str = "";
		boolean first = true;
		int i = 0;
		for (String s : frms.keySet()) {
			if (s.length() > 0) {
				ZFromItem f = frms.get(s);
				if (first) {
					// str
					// +=(" "+(clzReg.get(f.getTable())+" _"+(f.getAlias()==null?(f.getTable()):(f.getAlias()))));
					str += "org.ptg.events.Event evt" + i;
					first = false;
				} else {
					// str
					// +=(" "+(clzReg.get(f.getTable())+" _"+(f.getAlias()==null?(f.getTable()):(f.getAlias()))));
					str += ",org.ptg.events.Event evt" + i;
				}
			}
			i++;
		}
		return str;
	}

	public String getResultCollector() {
		String temp = "{";
		temp += getTableDependencyTypeCast();
		temp += "\n/*###############################################Returns#######################*/ \n" + "java.util.Map returns  = new java.util.HashMap();\n";
		for (Map.Entry<String, Pair> en : results.entrySet()) {
			if (en.getValue().getKey() == null) {
				if (depends.containsKey(en.getValue().getVal()) || aliasTables.containsKey(en.getValue().getVal())) {
					if (aliasTables.containsKey(en.getValue().getVal())) {
						temp += "returns.put(\"" + en.getKey() + "\",($w)" + en.getValue().getVal() + ");\n";
					} else {
						temp += "returns.put(\"" + en.getKey() + "\",($w)" + reverseAlias.get(en.getValue().getVal()) + ");\n";
					}
				} else {
					temp += MSG_Header;
					temp += "\n/*please make sure column names use FQN also \nmissing column must be part of table\n" + "we will assume it a function call to suggest domain build up:" + en.getKey()
							+ "*/\n";
					temp += "\n/*Assuming a function without provided parameters*/\n";
					temp += MSG_Header;
					temp += "returns.put(\"" + en.getKey() + "\",($w)" + en.getValue().getVal() + ");\n";
				}
			} else {
				if ("this".equals(en.getValue().getKey())) {
					temp += "returns.put(\"" + en.getKey() + "\",($w)" + getGetter(en.getValue().getKey() + "." + en.getValue().getVal()) + ");\n";

				} else {
					temp += "if (" + en.getValue().getKey() + "!=null){\nreturns.put(\"" + en.getKey() + "\",($w)" + getGetter(en.getValue().getKey() + "." + en.getValue().getVal()) + ");\n}\n";
				}
			}
		}
		temp += "return returns;\n}";
		return temp;
	}

	public void printStack() {
		/*
		 * int i = 0; for(String s: stk){ for(int j=0;j<i;j++)
		 * System.out.print("\t"); System.out.println(s); i++; }
		 */
	}

	private static class Pair {
		String key;
		String val;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getVal() {
			return val;
		}

		public void setVal(String val) {
			this.val = val;
		}

		public Pair(String key, String val) {

			this.key = key;
			this.val = val;
		}

		@Override
		public String toString() {
			return "Pair [key=" + key + ", val=" + val + "]";
		}

	}

	public String preprocess(String test) {
		Map<Integer, String> codestemp = new HashMap<Integer, String>();
		int count = 0;
		StringBuilder out = new StringBuilder("");
		State state = State.start;

		StringBuilder lastCode = new StringBuilder();
		for (char c : test.toCharArray()) {
			if (state != State.between) {
				out.append(c);
			} else {
				lastCode.append(c);
			}
			if (c == '^') {
				if (state == State.inquotes || state == State.insingle) {
				} else {
					state = State.between;
					out.deleteCharAt(out.length() - 1);
				}
			} else if (c == '~') {
				if (state == State.inquotes || state == State.insingle) {
				} else {
					state = State.start;
					lastCode.deleteCharAt(lastCode.length() - 1);
					codestemp.put(count, lastCode.toString());
					lastCode = new StringBuilder();
					out.append("  localfunc" + count + "('##$$%%$$##@@!!#@!!~~')");
					count = count + 1;

				}
			} else if (c == '\'') {
				if (state == State.insingle) {
					state = State.start;
				} else {
					if (state == State.between) {
					} else {
						state = State.insingle;
					}
				}
			} else if (c == '\"') {
				if (state == State.inquotes) {
					state = State.start;
				} else {
					if (state == State.between) {
					} else {
						state = State.inquotes;
					}
				}
			} else {

			}
		}
		// System.out.println(out);
		// System.out.println("\n\n\n\n" + codestemp);
		for (int i = 0; i < 10; i++) {
			dependTable.put("localfunc" + strNum.get(i), "this");
			dependParamTable.put("localfunc" + strNum.get(i), 1);
		}

		runQueryNoPreprocess(out.toString());
		codes = codestemp;
		String toReplace = getReplaceFuncStr();
		for (int i = 0; i < codes.size(); i++) {
			dependTable.put("localfunc" + strNum.get(i), "this");
			dependParamTable.put("localfunc" + strNum.get(i), frms.size());
		}

		String ret = StringUtils.replace(out.toString(), "'##$$%%$$##@@!!#@!!~~'", toReplace);
		return ret;
	}

	public String getReplaceFuncStr() {
		boolean notfound = true;
		String ret = "";
		for (ZFromItem i : frms.values()) {
			if (notfound) {
				ret += i.getAlias() == null ? i.getTable() : i.getAlias();
				notfound = false;
			} else {
				ret += ",";
				ret += i.getAlias() == null ? i.getTable() : i.getAlias();

			}
		}
		return ret;
	}

	public Map<Integer, String> getPartialCode() {
		return codes;
	}

	static enum State {
		start, between, insingle, inquotes;

	}

	public synchronized Map<String, ZFromItem> getFrms() {
		return frms;
	}

	public synchronized void setFrms(Map<String, ZFromItem> frms) {
		this.frms = frms;
	}

	public synchronized Map<String, String> getAliasTables() {
		return aliasTables;
	}

	public synchronized void setAliasTables(Map<String, String> aliasTables) {
		this.aliasTables = aliasTables;
	}

	public synchronized Map<String, Pair> getResults() {
		return results;
	}

	public synchronized void setResults(Map<String, Pair> results) {
		this.results = results;
	}

}