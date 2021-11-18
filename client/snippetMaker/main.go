package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/olekukonko/tablewriter"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// type Select struct {
// 	ColumnType     int    `json:"columnType"` // 1: (columnName), 2: (aggregateName,aggregateValue)
// 	ColumnName     string `json:"columnName"`
// 	AggregateName  string `json:"aggregateName"`
// 	AggregateValue string `json:"aggregateValue"`
// }

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
type CsdInfos struct {
	NodeTotal int    `json:"nodeTotal"`
	CsdTotal  int    `json:"csdTotal"`
	Items     []Item `json:"items"`
}
type Item struct {
	Node int            `json:"node"`
	Csd  map[string]int `json:"csd"`
}

type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    Data   `json:"data"`
}

type Data struct {
	// Table       string                         `json:"table"`
	Field         []string                       `json:"field"`
	Values        map[string][]map[string]string `json:"values"`
	GroupNames    []string                       `json:"groupNames"`
	NumOfGroupMap map[string]int                 `json:"NumOfGroupMap"`
	SelectWords   []SelectWord                   `json:"selectwords"`
}
type SelectWord struct {
	Operator   string
	Expression string
	//Column     string
	AsColumn string
}
type GData struct {
	GroupName string
	DataMap   map[string]string
}

func JSONMarshal(t interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(t)
	return buffer.Bytes(), err
}

func Marshal(i interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(i)
	return bytes.TrimRight(buffer.Bytes(), "\n"), err
}

func RequestSnippet(query string, SchedulerIP string, SchedulerPort string) {
	///////////////////////////
	startTime := time.Now()

	select_str, from_str, where_str, group_by_str, having_str, order_by_str := Parse(query)

	endTime := time.Since(startTime).Seconds()
	fmt.Printf("Parse : %0.1f sec\n", endTime)
	///////////////////////////

	///////////////////////////
	sectionStartTime := time.Now()

	snippet, err := MakeSnippet(from_str, where_str)
	if err != nil {
		log.Println(err)
		return
	}

	endTime = time.Since(sectionStartTime).Seconds()
	fmt.Printf("MakeSnippet : %0.1f sec\n", endTime)
	///////////////////////////

	///////////////////////////
	sectionStartTime = time.Now()

	json_snippet_byte, err := json.MarshalIndent(snippet, "", "  ")

	endTime = time.Since(sectionStartTime).Seconds()
	fmt.Printf("Marshal (Struct to []Byte) : %0.1f sec\n", endTime)

	if err != nil {
		//fmt.Println(err)
		return
	}
	///////////////////////////

	// 입력확인
	fmt.Println(string(json_snippet_byte))
	snippet_buff := bytes.NewBuffer(json_snippet_byte)

	req, err := http.NewRequest("GET", "http://"+SchedulerIP+":"+SchedulerPort, snippet_buff)

	if err != nil {
		//fmt.Println("httperr : ", err)
	} else {

		sectionStartTime := time.Now()

		client := &http.Client{}
		resp, errclient := client.Do(req)

		endTime := time.Since(sectionStartTime).Seconds()
		fmt.Printf("Fillter in CSD : %0.1f sec\n", endTime)

		if errclient != nil {
			//fmt.Println("resperr : ", errclient)
		} else {
			defer resp.Body.Close()

			bytes, _ := ioutil.ReadAll(resp.Body)
			jsonDataString := string(bytes)
			// jsonDataString = `{
			// 	"code": 200,
			// 	"message": "OK",
			// 	"data": {
			// 	  "table": "",
			// 	  "field": [
			// 		"L_ORDERKEY",
			// 		"L_PARTKEY",
			// 		"L_SUPPKEY",
			// 		"L_LINENUMBER",
			// 		"L_QUANTITY",
			// 		"L_EXTENDEDPRICE",
			// 		"L_DISCOUNT",
			// 		"L_TAX",
			// 		"L_RETURNFLAG",
			// 		"L_LINESTATUS",
			// 		"L_SHIPDATE",
			// 		"L_COMMITDATE",
			// 		"L_RECEIPTDATE",
			// 		"L_SHIPINSTRUCT",
			// 		"L_SHIPMODE",
			// 		"L_COMMENT"
			// 	  ],
			// 	  "values": {
			// 		"": [
			// 		  {
			// 			"L_ORDERKEY": "1",
			// 			"L_PARTKEY": "15519",
			// 			"L_SUPPKEY": "785",
			// 			"L_LINENUMBER": "1",
			// 			"L_QUANTITY": "17",
			// 			"L_EXTENDEDPRICE": "24386.67",
			// 			"L_DISCOUNT": "0.04",
			// 			"L_TAX": "0.02",
			// 			"L_RETURNFLAG": "A",
			// 			"L_LINESTATUS": "B",
			// 			"L_SHIPDATE": "1996.3.13",
			// 			"L_COMMITDATE": "1996.2.12",
			// 			"L_RECEIPTDATE": "1996.3.22",
			// 			"L_SHIPINSTRUCT": "DELIVER IN PERSON",
			// 			"L_SHIPMODE": "TRUCK",
			// 			"L_COMMENT": "egular courts above the"
			// 		  },
			// 		  {
			// 			"L_ORDERKEY": "1",
			// 			"L_PARTKEY": "15519",
			// 			"L_SUPPKEY": "785",
			// 			"L_LINENUMBER": "1",
			// 			"L_QUANTITY": "17",
			// 			"L_EXTENDEDPRICE": "24386.67",
			// 			"L_DISCOUNT": "0.04",
			// 			"L_TAX": "0.02",
			// 			"L_RETURNFLAG": "D",
			// 			"L_LINESTATUS": "D",
			// 			"L_SHIPDATE": "1996.3.13",
			// 			"L_COMMITDATE": "1996.2.12",
			// 			"L_RECEIPTDATE": "1996.3.22",
			// 			"L_SHIPINSTRUCT": "DELIVER IN PERSON",
			// 			"L_SHIPMODE": "TRUCK",
			// 			"L_COMMENT": "egular courts above the"
			// 		  },
			// 		  {
			// 			"L_ORDERKEY": "1",
			// 			"L_PARTKEY": "15519",
			// 			"L_SUPPKEY": "785",
			// 			"L_LINENUMBER": "1",
			// 			"L_QUANTITY": "17",
			// 			"L_EXTENDEDPRICE": "24386.67",
			// 			"L_DISCOUNT": "0.04",
			// 			"L_TAX": "0.02",
			// 			"L_RETURNFLAG": "A",
			// 			"L_LINESTATUS": "B",
			// 			"L_SHIPDATE": "1996.3.13",
			// 			"L_COMMITDATE": "1996.2.12",
			// 			"L_RECEIPTDATE": "1996.3.22",
			// 			"L_SHIPINSTRUCT": "DELIVER IN PERSON",
			// 			"L_SHIPMODE": "TRUCK",
			// 			"L_COMMENT": "egular courts above the"
			// 		  }
			// 		]
			// 	  }
			// 	}
			//   }`

			res := resJsonParser(jsonDataString)
			if res.Code == 200 {
				filltered_res := Fillter(res, select_str, group_by_str, having_str, order_by_str)
				/*
					Print Result Json Data
				*/
				// filltered_res_byte, _ := json.MarshalIndent(filltered_res, "", "  ")

				// fmt.Println("\n[ Result ]")
				// fmt.Println(string(filltered_res_byte))
				/*
					Print Result Json Data End
				*/
				printClient(filltered_res)

			} else {
				log.Fatal(res.Message)
			}

		}
	}
	endTime = time.Since(startTime).Seconds()
	fmt.Printf("Total : %0.1f sec\n", endTime)

}
func getTableSchema(tableName string) TableSchema {
	// TODO 스키마 데이터 로드하는 형식으로 바꿔야함 (queryEngine or ddl)
	schema := make(map[string]TableSchema)
	schema["nation"] = TableSchema{
		ColumnNames: []string{"N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"},
		ColumnTypes: []string{"int", "char", "int", "char"}, // int, char, varchar, TEXT, DATETIME,  ...
		ColumnSizes: []int{8, 25, 8, 152},                   // Data Size}
	}
	schema["region"] = TableSchema{
		ColumnNames: []string{"R_REGIONKEY", "R_NAME", "R_COMMENT"},
		ColumnTypes: []string{"int", "char", "varchar"}, // int, char, varchar, TEXT, DATETIME,  ...
		ColumnSizes: []int{8, 8, 152},                   // Data Size}
	}
	schema["part"] = TableSchema{
		ColumnNames: []string{"P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"},
		ColumnTypes: []string{"int", "varchar", "char", "char", "varchar", "int", "char", "decimal(15,2)", "varchar"}, // int, char, varchar, TEXT, DATETIME,  ...
		ColumnSizes: []int{8, 55, 25, 10, 25, 8, 10, 15, 101},                                                         // Data Size}
	}
	schema["supplier"] = TableSchema{
		ColumnNames: []string{"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"},
		ColumnTypes: []string{"int", "char", "varchar", "int", "char", "decimal(15,2)", "varchar"}, // int, char, varchar, TEXT, DATETIME,  ...
		ColumnSizes: []int{8, 25, 40, 8, 15, 15, 101},                                              // Data Size}
	}
	schema["partsupp"] = TableSchema{
		ColumnNames: []string{"PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"},
		ColumnTypes: []string{"int", "varchar", "varchar", "int", "char", "decimal(15,2)", "char", "varchar"}, // int, char, varchar, TEXT, DATETIME,  ...
		//ColumnSizes: []int{110325, 110325, 110325, 110325},  // Data Size}
		ColumnSizes: []int{8, 25, 40, 8, 15, 15, 10, 117}, // Data Size}
	}
	schema["customer"] = TableSchema{
		ColumnNames: []string{"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"},
		ColumnTypes: []string{"int", "varchar", "varchar", "int", "char", "decimal(15,2)", "char", "varchar"}, // int, char, varchar, TEXT, DATETIME,  ...
		ColumnSizes: []int{8, 25, 40, 8, 15, 15, 10, 117},                                                     // Data Size}
	}
	schema["orders"] = TableSchema{
		ColumnNames: []string{"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"},
		ColumnTypes: []string{"int", "int", "char", "decimal(15,2)", "date", "char", "char", "int", "varchar"}, // int, char, varchar, TEXT, DATETIME,  ...
		ColumnSizes: []int{8, 8, 1, 15, -1, 15, 15, 8, 79},                                                     // Data Size}
	}
	schema["lineitem"] = TableSchema{
		ColumnNames: []string{"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"},
		ColumnTypes: []string{"int", "int", "int", "int", "decimal(15,2)", "decimal(15,2)", "decimal(15,2)", "decimal(15,2)", "char", "char", "date", "date", "date", "char", "char", "varchar"}, // int, char, varchar, TEXT, DATETIME,  ...
		ColumnSizes: []int{8, 8, 8, 8, 15, 15, 15, 15, 1, 1, -1, -1, 25, 10, 44},                                                                                                                 // Data Size}
	}

	return schema[tableName]
}
func Parse(query string) (string, string, string, string, string, string) {
	//query.replace(" ", ' ')
	//log.Println(query)
	query = strings.TrimSuffix(query, ";")
	querySlice := strings.Split(query, " ")
	select_str := ""
	from_str := ""
	where_str := ""
	group_by_str := ""
	having_str := ""
	order_by_str := ""

	index := ""
	// whereSlice := make([]string, 3)
	// operatorFlag := false

	//flag := 0
	groupby := false
	orderby := false

	for i, atom := range querySlice {
		//fmt.Println(atom)
		if strings.ToLower(atom) == "select" {
			index = "select"
			continue
		} else if strings.ToLower(atom) == "from" {
			index = "from"
			//klog.Infoln("Current Index", index)
			continue
		} else if strings.ToLower(atom) == "where" {
			index = "where"
			continue
		} else if strings.ToLower(atom) == "group" && strings.ToLower(querySlice[i+1]) == "by" {
			groupby = true
			index = "group by"
			continue
		} else if strings.ToLower(atom) == "having" {
			index = "having"
			continue
		} else if strings.ToLower(atom) == "order" && strings.ToLower(querySlice[i+1]) == "by" {
			orderby = true
			index = "order by"
			continue
		} else if groupby || orderby {
			groupby = false
			orderby = false
			continue

		}

		//fmt.Println(index)
		switch index {
		case "select":
			select_str = select_str + " " + atom
		case "from":
			from_str = from_str + " " + atom
		case "where":
			where_str = where_str + " " + atom
		case "group by":
			group_by_str = group_by_str + " " + atom
		case "having":
			having_str = having_str + " " + atom
		case "order by":
			order_by_str = order_by_str + " " + atom
		}

	}

	//fmt.Println("select_str:", select_str)
	//fmt.Println("from_str:", from_str)
	//fmt.Println("where_str:", where_str)
	//fmt.Println("group_by_str:", group_by_str)
	//fmt.Println("having_str:", having_str)
	//fmt.Println("order_by_str:", order_by_str)
	return select_str, from_str, where_str, group_by_str, having_str, order_by_str
}

type CSDDaemon struct {
	NodeNum    int
	CSDNum     int
	isAvailble bool
}

func getCSDInfos() CsdInfos {

	CSDDaemons := []CSDDaemon{
		{
			NodeNum:    1,
			CSDNum:     1,
			isAvailble: true,
		},
		{
			NodeNum:    2,
			CSDNum:     2,
			isAvailble: true,
		},
		// {
		// 	NodeNum:    2,
		// 	CSDNum:     3,
		// 	isAvailble: false,
		// },
		// {
		// 	NodeNum:    2,
		// 	CSDNum:     4,
		// 	isAvailble: true,
		// },
	}

	nodeTotal := 0
	csdTotal := 0

	nodeTotalMap := make(map[int]bool)
	items := []Item{}
	cnt := 0

	csdSeqMap := make(map[string]map[string]int)
	for _, cd := range CSDDaemons {
		if _, val := nodeTotalMap[cd.NodeNum]; !val {
			nodeTotalMap[cd.NodeNum] = val
			nodeTotal += 1

			csdSeqMap[strconv.Itoa(cd.NodeNum)] = make(map[string]int)
		}
		if cd.isAvailble {
			csdTotal += 1
		}

		if cd.isAvailble {
			csdSeqMap[strconv.Itoa(cd.NodeNum)][strconv.Itoa(cd.CSDNum)] = cnt
			cnt += 1

		}

	}
	for nodeNum, _ := range csdSeqMap {

		n, _ := strconv.Atoi(nodeNum)

		item := Item{
			Node: n,
			Csd:  csdSeqMap[nodeNum],
		}
		items = append(items, item)

	}

	cs := CsdInfos{
		NodeTotal: nodeTotal,
		CsdTotal:  csdTotal,
		Items:     items,
	}
	return cs
}
func getBlockOffset(from_str, where_str string) int {
	s := 312476
	return s
}

func getBufferAddress() string {
	s := "0x0847583"
	return s
}
func MakeSnippet(from_str, where_str string) (Snippet, error) {

	snippet := Snippet{
		TableNames:    make([]string, 0),
		TableSchema:   make(map[string]TableSchema),
		WhereClauses:  make([]Where, 0),
		BlockOffset:   getBlockOffset(from_str, where_str),
		BufferAddress: getBufferAddress(),
		CsdInfos:      getCSDInfos(),
	}

	for _, tableName := range strings.Split(from_str, ",") {
		tableName = strings.TrimSpace(tableName)
		snippet.TableNames = append(snippet.TableNames, tableName)

		tableSchema := getTableSchema(tableName)
		snippet.TableSchema[tableName] = tableSchema
	}

	operatorSequence := []string{}
	condtionSequence := []string{}
	where_str = strings.ToLower(where_str)

	tmp := ""
	for _, word := range strings.Split(where_str, " ") {
		if word == "and" {
			operatorSequence = append(operatorSequence, word)
			condtionSequence = append(condtionSequence, tmp)
			tmp = ""
		} else if word == "or" {
			operatorSequence = append(operatorSequence, word)
			condtionSequence = append(condtionSequence, tmp)
			tmp = ""
		} else {
			tmp = tmp + " " + word
		}

	}
	condtionSequence = append(condtionSequence, tmp)

	operatorSequence_index := 0

	for _, cond := range condtionSequence {
		//fmt.Println("cond:", cond)
		for _, atom := range strings.Split(cond, " ") {
			//fmt.Println("atom:", atom)
			if ok, exp := isEXP(atom); ok {
				whereSlice := strings.Split(cond, exp)
				whereSlice[0] = strings.TrimSpace(whereSlice[0])
				whereSlice[1] = strings.TrimSpace(whereSlice[1])
				//fmt.Println("exp:", exp)
				w := Where{
					LeftValue:       whereSlice[0],
					CompOperator:    exp,
					RightValue:      whereSlice[1],
					LogicalOperator: "NULL",
				}
				if operatorSequence_index < len(operatorSequence) {
					w.LogicalOperator = operatorSequence[operatorSequence_index]
					operatorSequence_index += 1
				}
				snippet.WhereClauses = append(snippet.WhereClauses, w)
			}

		}

	}

	//klog.Infoln(*request)
	////fmt.Println("snippet:")

	return snippet, nil
}

func do_group_by(res Response, group_by_str string) Response {

	group_by_res := res
	group_by_res.Data.Values = make(map[string][]map[string]string)

	for _, atom := range strings.Split(group_by_str, ",") {
		groupName := strings.TrimSpace(atom)
		groupName = strings.ToUpper(groupName)
		group_by_res.Data.GroupNames = append(group_by_res.Data.GroupNames, groupName)
	}

	if len(group_by_res.Data.GroupNames) == 0 {
		return res
	}

	for _, group := range res.Data.Values {
		for _, record := range group {
			groupTotalName := ""
			for _, groupName := range group_by_res.Data.GroupNames {
				groupTotalName = groupTotalName + " " + record[groupName]
			}

			groupTotalName = strings.TrimLeft(groupTotalName, " ")

			if _, ok := group_by_res.Data.Values[groupTotalName]; !ok {
				group_by_res.Data.Values[groupTotalName] = make([]map[string]string, 0)
			}
			group_by_res.Data.Values[groupTotalName] = append(group_by_res.Data.Values[groupTotalName], record)
		}

	}

	group_by_res.Data.NumOfGroupMap = make(map[string]int)

	for k := range group_by_res.Data.Values {
		groupTotalName := k

		numOfGroupMap := len(group_by_res.Data.Values[groupTotalName])
		group_by_res.Data.NumOfGroupMap[groupTotalName] = numOfGroupMap
		// fmt.Println("*************")
		// fmt.Println(groupTotalName)
		// fmt.Println(numOfGroupMap)
	}

	// for _, groupTotalName := range group_by_res.Data.GroupNames {
	// 	group_by_res.Data.NumOfGroupMap = make(map[string]int)
	// 	numOfGroupMap := len(group_by_res.Data.Values[groupTotalName])
	// 	group_by_res.Data.NumOfGroupMap[groupTotalName] = numOfGroupMap
	// 	fmt.Println("*************")
	// 	fmt.Println(groupTotalName)
	// 	fmt.Println(group_by_res.Data.Values[groupTotalName], numOfGroupMap)
	// }
	// //fmt.Println("group_by_res:", group_by_res)

	return group_by_res
}

func do_select(res Response, select_str string) Response {

	//selected_datas := [][]string{}

	asColumns := []string{}
	selected_res := res

	selected_res.Data.SelectWords = make([]SelectWord, 0)

	for _, atom := range strings.Split(select_str, ",") {
		operator := ""
		expression := ""
		asColumn := ""
		//column := ""

		if strings.Contains(atom, " as ") {
			index := strings.Index(atom, " as ")
			aggregater := strings.TrimLeft(atom[:index], " ")

			sp_aggregater := strings.Split(aggregater, "(")

			if len(sp_aggregater) > 1 {
				operator = sp_aggregater[0]
				index2 := strings.Index(aggregater, operator) + len(operator)
				expression = aggregater[index2:]
				//column = ""
			} else {
				operator = ""
				expression = ""
				//column = strings.TrimSpace(atom[:index])
			}

			asColumn = strings.TrimSpace(atom[index+4:])

		} else {
			aggregater := strings.TrimLeft(atom, " ")
			sp_aggregater := strings.Split(aggregater, "(")

			if len(sp_aggregater) > 1 {
				operator = sp_aggregater[0]
				index2 := strings.Index(aggregater, operator) + len(operator)
				expression = aggregater[index2:]
			} else {
				operator = ""
				expression = ""
			}

			asColumn = strings.TrimSpace(atom)

		}
		asColumns = append(asColumns, asColumn)

		sw := SelectWord{
			Operator:   operator,
			Expression: expression,
			//Column:     column,
			AsColumn: asColumn,
		}
		selected_res.Data.SelectWords = append(selected_res.Data.SelectWords, sw)

		//fmt.Println("operator: "+operator, "/ expression: "+expression, "/ asColumn: "+asColumn)

	}

	selected_res.Data.Field = asColumns

	if res.Data.GroupNames[0] == "" {
		return selected_res
	}

	selected_res.Data.Values = make(map[string][]map[string]string)
	for k := range res.Data.Values {
		selected_res.Data.Values[k] = make([]map[string]string, 1)

	}

	//log.Println("len: ", len(res.Data.Values))

	var wg sync.WaitGroup
	// chGroupName := make(chan string, len(res.Data.Values))
	// chData := make(chan map[string]string, len(res.Data.Values))

	numRecord := 0
	for groupName := range res.Data.Values {
		numRecord += res.Data.NumOfGroupMap[groupName]
	}
	wg.Add(numRecord)

	ch2 := make(chan GData, numRecord)

	for groupName, groupDatas := range res.Data.Values {

		for _, groupData := range groupDatas {

			go func(groupName string, groupData map[string]string, ch2 chan GData) {
				defer wg.Done()

				//log.Println("Go Routine")

				tmpGDataMap := make(map[string]string)

				parameters := make(map[string]interface{}, 8)
				for k, v := range groupData {
					//fmt.Println("v :", strings.ToLower(k), v)

					if f, err := strconv.ParseFloat(v, 64); err == nil {
						parameters[strings.ToLower(k)] = f
					} else if f, err := strconv.Atoi(v); err == nil {
						parameters[strings.ToLower(k)] = f
					} else {
						parameters[strings.ToLower(k)] = v
					}
				}
				for _, selectword := range selected_res.Data.SelectWords {
					if selectword.Operator == "" && selectword.Expression == "" {

						tmpGDataMap[strings.ToUpper(selectword.AsColumn)] = groupData[strings.ToUpper(selectword.AsColumn)]

					} else {
						var result interface{}

						//fmt.Println("[selectword.Expression]", selectword.Expression)
						if selectword.Expression == "(*)" {
							result = 1
						} else {
							expression, err := govaluate.NewEvaluableExpression(selectword.Expression)
							if err != nil {
								//fmt.Println(err)
							}
							result, err = expression.Evaluate(parameters)
							if err != nil {
								//fmt.Println(err)
							}
							//fmt.Println("result:", result)
						}

						res_str := fmt.Sprintf("%v", result)
						//fmt.Println("res_str:", res_str)

						tmpGDataMap[strings.ToUpper(selectword.AsColumn)] = res_str

					}
				}
				gData := GData{
					GroupName: groupName,
					DataMap:   tmpGDataMap,
				}

				ch2 <- gData

			}(groupName, groupData, ch2)

		}
	}

	//wg.Wait()
	// GDataMap := make(map[string]string)

	//log.Println("Go Routine End")
	for i := 0; i < numRecord; i++ {
		gDataMap := <-ch2

		// fmt.Println(gDataMap)

		groupName := gDataMap.GroupName
		groupDataMap := gDataMap.DataMap

		//fmt.Println(tmpGDataMap)

		for _, selectword := range selected_res.Data.SelectWords {

			if len(selected_res.Data.Values[groupName][0]) == 0 {
				selected_res.Data.Values[groupName][0] = make(map[string]string)
			}

			if _, ok := selected_res.Data.Values[groupName][0][strings.ToUpper(selectword.AsColumn)]; !ok {

				selected_res.Data.Values[groupName][0][strings.ToUpper(selectword.AsColumn)] = groupDataMap[strings.ToUpper(selectword.AsColumn)]

			} else {
				if selectword.Operator == "" && selectword.Expression == "" {
					continue
				}

				f1, err := strconv.ParseFloat(selected_res.Data.Values[groupName][0][strings.ToUpper(selectword.AsColumn)], 64)
				if err != nil {
					log.Println("error:", err)
				}
				f2, err := strconv.ParseFloat(groupDataMap[strings.ToUpper(selectword.AsColumn)], 64)
				if err != nil {
					log.Println("error:", err)
				}

				total_res_str := ""
				if selectword.Operator == "sum" {
					f := f1 + f2
					total_res_str = strconv.FormatFloat(f, 'f', 5, 64)
				} else if selectword.Operator == "avg" {
					f := f1 + f2
					total_res_str = strconv.FormatFloat(f, 'f', 5, 64)
				} else if selectword.Operator == "count" {
					f := f1 + f2 // count 인경우 1이기때문에 더하기
					total_res_str = strconv.FormatFloat(f, 'f', 5, 64)
				}
				selected_res.Data.Values[groupName][0][strings.ToUpper(selectword.AsColumn)] = total_res_str
			}

			// log.Println("GDataMap:", GDataMap)

		}
		// selected_res.Data.Values[groupName][0] = GDataMap
	}

	//fmt.Println("ee", dataMap)
	for groupName := range selected_res.Data.Values {
		for _, selectword := range selected_res.Data.SelectWords {
			if selectword.Operator == "avg" {
				tmp := selected_res.Data.Values[groupName][0][strings.ToUpper(selectword.AsColumn)]
				f1, err := strconv.ParseFloat(tmp, 64)
				if err != nil {
					log.Println("error:", err)
				}
				f := f1 / float64(selected_res.Data.NumOfGroupMap[groupName])
				total_res_str := strconv.FormatFloat(f, 'f', 5, 64)

				selected_res.Data.Values[groupName][0][strings.ToUpper(selectword.AsColumn)] = total_res_str
			}
		}
	}

	// 	}

	// }

	//fmt.Println("ee2")

	//selected_res.Data.Values[groupName] = append(selected_res.Data.Values[groupName], GDataMap)

	// chGroupName <- groupName
	// chData <- GDataMap

	//	}

	wg.Wait() // 모든 고루틴이 종료될 때까지 대기
	fmt.Println("Ended Goroutine")

	// for i := 0; i < len(res.Data.Values); i++ {
	// 	groupName := <-chGroupName
	// 	GDataMap := <-chData

	// 	selected_res.Data.Values[groupName] = append(selected_res.Data.Values[groupName], GDataMap)
	// }

	//fmt.Println(selected_res.Data.Values)

	// for _, group := range res.Data.Values {
	// 	for _, record := range group {
	// 		groupTotalName := ""
	// 		for _, groupName := range group_by_res.Data.GroupNames {
	// 		}
	// 	}
	// }

	//fmt.Println("fields:", asColumns)

	return selected_res
}
func do_order_by(res Response, order_by_str string) Response {

	order_by_res := res
	order_by_res.Data.Values = make(map[string][]map[string]string)

	// order by
	mapDatas := make([]map[string]string, 0)
	for _, groupDatas := range res.Data.Values {
		mapDatas = append(mapDatas, groupDatas...)
	}

	s := strings.Split(order_by_str, ",")

	for k, _ := range s {
		k = len(s) - 1 - k
		atom := s[k]

		order := "ASC"
		colName := ""
		atom = strings.TrimSpace(atom)

		if strings.Contains(strings.ToUpper(atom), " DESC") || strings.Contains(strings.ToUpper(atom), " ASC") {
			index := strings.Index(atom, " ")

			colName = atom[:index]
			colName = strings.ToUpper(colName)
			order = strings.ToUpper(atom[index+1:])

		} else {
			colName = strings.ToUpper(atom)
		}
		// fmt.Println("order:", order)
		// fmt.Println("colName:", colName)

		if order == "ASC" {
			sort.Slice(mapDatas, func(i, j int) bool { return mapDatas[i][colName] < mapDatas[j][colName] }) // 오름차순
		} else if order == "DESC" {
			sort.Slice(mapDatas, func(i, j int) bool { return mapDatas[i][colName] > mapDatas[j][colName] }) // 내림차순
		}

	}

	//fmt.Println("mapData:", mapDatas)

	order_by_res.Data.Values[""] = mapDatas

	return order_by_res

}
func do_convert_data(res Response) Response {
	// tableName := res.Data.Table
	// schema := getTableSchema(tableName)

	// for _, group := range res.Data.Values {
	// 	for _, record := range group {
	// 		for k, v := range record {
	// 			schema.ColumnNames
	// 		}
	// 	}
	// }
	// schema.ColumnNames
	return res

}
func Fillter(res Response, select_str, group_by_str, having_str, order_by_str string) Response {

	// res_byte, _ := json.MarshalIndent(res, "", "  ")

	// fmt.Println("\n[ res_byte ]")
	// fmt.Println(string(res_byte))

	// res = do_convert_data(res)

	startTime := time.Now()

	res = do_group_by(res, group_by_str)

	endTime := time.Since(startTime).Seconds()
	fmt.Printf("do_group_by: %0.1f sec\n", endTime)

	startTime = time.Now()

	res = do_select(res, select_str)

	endTime = time.Since(startTime).Seconds()
	fmt.Printf("do_select: %0.1f sec\n", endTime)

	// res = do_having(res, having_str)

	startTime = time.Now()

	res = do_order_by(res, order_by_str)

	endTime = time.Since(startTime).Seconds()
	fmt.Printf("do_order_by: %0.1f sec\n", endTime)

	return res
}
func isIntegral(val float64) bool {
	return val == float64(int(val))
}
func SplitAny(s string, seps string) []string {
	splitter := func(r rune) bool {
		return strings.ContainsRune(seps, r)
	}
	return strings.FieldsFunc(s, splitter)
}

func resJsonParser(jsonDataString string) Response {
	var res Response

	// jsonDataString = strings.Replace(jsonDataString, "No Servers Available", "", -1)

	if err := json.Unmarshal([]byte(jsonDataString), &res); err != nil {
		log.Fatal(err)
	}

	return res
}

func isAggregateFunc(atom string) (bool, string) {
	aggregateList := []string{"count", "sum", "avg", "max", "min"}
	for _, aggregater := range aggregateList {
		if strings.Contains(atom, aggregater+"(") {
			return true, aggregater
		}
	}
	return false, ""
}
func isOperator(atom string) (bool, string) {
	opList := []string{"and", "or"}
	for _, op := range opList {
		atom = strings.ToLower(atom)
		if atom == op {
			return true, op
		}
	}
	return false, ""
}
func isEXP(atom string) (bool, string) {
	expList := []string{">=", "<=", ">", "<", "="}
	for _, exp := range expList {
		if strings.Contains(atom, exp) {
			return true, exp
		}
	}
	return false, ""
}

func printClient(res Response) {
	datas := [][]string{}
	//fields := []string{}

	for _, groupDatas := range res.Data.Values {

		for _, groupData := range groupDatas {
			data := []string{}
			for _, field := range res.Data.Field {
				data = append(data, string(groupData[strings.ToUpper(field)]))
			}
			datas = append(datas, data)
		}

	}

	//fmt.Println("datas:", datas)
	//fmt.Println()

	table := tablewriter.NewWriter(os.Stdout)

	table.SetHeader(res.Data.Field)
	table.SetBorder(true)
	table.SetAutoFormatHeaders(false)
	table.SetCaption(true, "Total: "+strconv.Itoa(len(datas)))
	table.AppendBulk(datas)
	table.Render()

}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile)
	SchedulerIP := "10.0.5.101"
	SchedulerPort := "8100"

	// // SchedulerIP = "www.naver.com"
	// // SchedulerPort = "80"

	// query := "SELECT emp_no, first_name FROM employees WHERE hire_date>='1999-12-23'"
	// query := "SELECT sum(C_CUSTKEY) FROM customer WHERE C_NAME='a' and C_CUSTKEY='1' and C_CUSTKEY='2' and C_CUSTKEY='3'"
	// query := "SELECT C_NAME, C_ADDRESS, C_PHONE, C_CUSTKEY FROM customer WHERE C_CUSTKEY=525"
	// query := "SELECT C_CUSTKEY FROM customer WHERE C_CUSTKEY < 500"
	// // query := "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '108' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
	//query := "select l_returnflag, l_linestatus, L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_SHIPDATE, L_COMMITDATE from lineitem where l_shipdate <= date '1998-12-01' - interval '108' day order by l_returnflag, l_linestatus;"
	query := "select l_returnflag, l_linestatus,L_TAX, L_SHIPDATE, L_COMMITDATE from lineitem where l_shipdate <= date '1998-12-01' - interval '108' day;"
	query = "select p_parkey, s_acctbal, n_name from part, supplier, partsupp, nation, region where p_partkey = ps_partkey;"

	tpchQuery := make(map[int]string)

	tpchQuery[1] = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '108' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
	tpchQuery[2] = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = (select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE') order by s_acctbal desc, n_name, s_name, p_partkey LIMIT 100;"
	tpchQuery[3] = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate LIMIT 10;"
	tpchQuery[4] = "select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month and exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority;"
	tpchQuery[5] = "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year group by n_name order by revenue desc;"
	tpchQuery[6] = "select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between .06 - 0.01 and .06 + 0.01 and l_quantity < 24;"
	tpchQuery[7] = "select supp_nation, cust_nation, l_year, sum(volume) as revenue from (select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')) and l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year;"
	tpchQuery[8] = "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from (select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nations group by o_year order by o_year;"
	tpchQuery[9] = "select nation, o_year, sum(amount) as sum_profit from (select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%') as profit group by nation, o_year order by nation, o_year desc;"
	tpchQuery[10] = "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc LIMIT 20;"
	tpchQuery[11] = "select ps_partkey, sum(ps_supplycost * ps_availqty) as 'VALUE' from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > (select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY') order by 'VALUE' desc;"
	tpchQuery[12] = "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year group by l_shipmode order by l_shipmode;"
	tpchQuery[13] = "select c_count, count(*) as custdist from (select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;"
	tpchQuery[14] = "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;"
	tpchQuery[15] = "with revenue0 (supplier_no, total_revenue) as (select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month group by l_suppkey) select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier, revenue0 where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue0 ) order by s_suppkey;"
	tpchQuery[16] = "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in (select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size;"
	tpchQuery[17] = "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey );"
	tpchQuery[18] = "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate LIMIT 100;"
	tpchQuery[19] = "select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' );"
	tpchQuery[20] = "select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year ) ) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;"
	tpchQuery[21] = "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name LIMIT 100;"
	tpchQuery[22] = "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer where substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;"

	//query := tpchQuery[1]
	RequestSnippet(query, SchedulerIP, SchedulerPort)

}
