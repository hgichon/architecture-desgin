package types

type Snippet struct {
	TableNames    []string               `json:"tableNames"`
	TableSchema   map[string]TableSchema `json:"tableSchema"`
	WhereClauses  []Where                `json:"whereClause"`
	BlockOffset   int                    `json:"blockOffset"`
	BufferAddress string                 `json:"bufferAddress"`
	CsdInfos      CsdInfos               `json:"csdInfos"`
}

type Where struct {
	LeftValue       string `json:"leftValue"`
	CompOperator    string `json:"compOperator"`
	RightValue      string `json:"rightValue"`
	LogicalOperator string `json:"logicalOperator"`
}

type TableSchema struct {
	ColumnNames []string `json:"columnNames"`
	ColumnTypes []string `json:"columnTypes"`
	ColumnSizes []int    `json:"columnSizes"`
}

type Item struct {
	Node int            `json:"node"`
	Csd  map[string]int `json:"csd"`
}

type CsdInfos struct {
	NodeTotal int    `json:"nodeTotal"`
	CsdTotal  int    `json:"csdTotal"`
	Items     []Item `json:"items"`
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

type QueryResponse struct {
	TableNames    []string `json:"table"`
	BufferAddress string   `json:"bufferAddress"`
	// Field         []string            `json:"field"`
	TableData map[string]TableValues `json:"tableData"`
}

type TableValues struct {
	Values []map[string]string `json:"values"`
}

type ScanData struct {
	Snippet   Snippet                `json:"snippet"`
	Tabledata map[string]TableValues `json:"tabledata"`
}
