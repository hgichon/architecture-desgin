package types

type Snippet struct {
	// Parsedquery ParsedQuery `json:"parsedQuery"`
	// TableSchema TableSchema `json:"tableSchema"`
	// BlockOffset int         `json:"blockOffset"` // 파일위치
	// BufferAddress string    `json:"bufferAddress"`
	TableNames    []string               `json:"tableNames"`
	TableSchema   map[string]TableSchema `json:"tableSchema"`
	WhereClauses  []Where                `json:"whereClause"`
	BlockOffset   int                    `json:"blockOffset"`
	BufferAddress string                 `json:"buggerAddress"`
}

// type ParsedQuery struct {
// 	TableName    string   `json:"tableName"`
// 	Columns      []Select `json:"columnName"`
// 	WhereClauses []Where  `json:"whereClause"`
// }
type Select struct {
	ColumnType     int    `json:"columnType"` // 1: (columnName), 2: (aggregateName,aggregateValue)
	ColumnName     string `json:"columnName"`
	AggregateName  string `json:"aggregateName"`
	AggregateValue string `json:"aggregateValue"`
}
type Where struct {
	LeftValue       string `json:"leftValue"`
	CompOperator    string `json:"compOperator"`
	RightValue      string `json:"rightValue"`
	LogicalOperator string `json:"logicalOperator"`
}
type TableSchema struct {
	ColumnNames []string `json:"columnNames"`
	ColumnTypes []string `json:"columnTypes"` // int, char, varchar, TEXT, DATETIME,  ...
	ColumnSizes []int    `json:"columnSizes"` // Data Size
}

type QueryResponse struct {
	Table []string `json:"table"`
	// BufferAddress string   `json:"bufferAddress"`
	Field []string `json:"field"`
	// Values map[string]TableValues `json:"values"`
	Values map[string][]map[string]string `json:"values"`
	// TableData map[string]TableValues         `json:"tableData"`
}

type TableValues struct {
	Values []map[string]string `json:"values"`
}

type Data struct {
	Table       []string                       `json:"table"`
	Field       []string                       `json:"feild"`
	Values      map[string][]map[string]string `json:"values"`
	GroupNames  []string                       `json:"groupNames"`
	SelectWords []string                       `json:"selectwords"`
}

type ScanData struct {
	Snippet   Snippet                `json:"snippet"`
	Tabledata map[string]TableValues `json:"tabledata"`
}

type FilterData struct {
	Result   QueryResponse          `json:"result"`
	TempData map[string]TableValues `json:"tempData"`
}

type ResponseA struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    Data   `json:"data"`
}
