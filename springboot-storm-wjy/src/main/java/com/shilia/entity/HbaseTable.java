package com.shilia.entity;

public class HbaseTable {
	private String qualifier;
	private String family;
	private String value;
	private String rowKey;
	private String timestamp;
	private String tableName;
	public String getQualifier() {
		return qualifier;
	}
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}
	public String getFamily() {
		return family;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getRowKey() {
		return rowKey;
	}
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	@Override
	public String toString() {
		return "HbaseTable [qualifier=" + qualifier + ", family=" + family + ", value=" + value + ", rowKey=" + rowKey
				+ ", timestamp=" + timestamp + ", tableName=" + tableName + "]";
	}
	
	
	
}
