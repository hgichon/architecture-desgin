package types

type Snippet struct {
	Parsedquery ParsedQuery `json:"parsedQuery"`
	TableSchema TableSchema `json:"tableSchema"`
	BlockOffset int         `json:"blockOffset"` // 파일위치
	BufferAddress string    `json:"bufferAddress"`
}
type ParsedQuery struct {
	TableName    string   `json:"tableName"`
	Columns      []Select `json:"columnName"`
	WhereClauses []Where  `json:"whereClause"`
}
type Select struct {
	ColumnType     int    `json:"columnType"` // 1: (columnName), 2: (aggregateName,aggregateValue)
	ColumnName     string `json:"columnName"`
	AggregateName  string `json:"aggregateName"`
	AggregateValue string `json:"aggregateValue"`
}
type Where struct {
	LeftValue  string `json:"leftValue"`
	Exp        string `json:"exp"`
	RightValue string `json:"rightValue"`
	Operator   string `json:"operator"` // "AND": 뒤에 나오는 Where은 And조건, "OR": 뒤에 나오는 Where은 OR 조건, "NULL": 뒤에 나오는 조건 없음
}
type TableSchema struct {
	ColumnNames []string `json:"columnNames"`
	ColumnTypes []string `json:"columnTypes"` // int, char, varchar, TEXT, DATETIME,  ...
	ColumnSizes []int    `json:"columnSizes"` // Data Size
}

type QueryResponse struct {
	Table  string              `json:"table"`
	BufferAddress  string      `json:"bufferAddress"`
	Field  []string            `json:"field"`
	Values []map[string]string `json:"values"`
}
