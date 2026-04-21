package main

// Data types in redis

type DataType string

const (
	String    DataType = "string"
	List      DataType = "list"
	None      DataType = "none"
	Stream_DT DataType = "stream"
)
