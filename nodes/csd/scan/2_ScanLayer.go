package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"strconv"

	// "encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	types "scan/type"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// const rootDirectory = "/home/ngd/workspace/usr/kch/ditributed/nodes/csd/data/csv/"

// const rootDirectory = "/root/workspace/usr/coyg/module/tpch/"
const rootDirectory = "/root/workspace/usr/kch/distributed/nodes/csd/data/csv/"
const myCsdNum = 1

type ScanData struct {
	Snippet   types.Snippet                `json:"snippet"`
	Tabledata map[string]types.TableValues `json:"tabledata"`
}

//데이터 파일 읽어옴
func Scan(w http.ResponseWriter, r *http.Request) {
	first := time.Now()
	st := time.Now()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		//klog.Errorln(err)
		log.Println(err)
	}
	recieveData := &types.Snippet{}
	err = json.Unmarshal(body, recieveData)
	if err != nil {
		//klog.Errorln(err)
		log.Println(err)
	}
	log.Println("Marshall", time.Since(st).Seconds(), "SEC")

	data := recieveData
	log.Println("recieveData", data)
	log.Println("CSD info", data.CsdInfos)

	// 테이블 명 모두 소문자로 변경
	// log.Println(data.TableNames)
	// var tblArr []string
	// for _, i := range data.TableNames {
	// 	tblArr = append(tblArr, strings.ToLower(i))
	// }
	// data.TableNames = tblArr
	log.Println(data.TableNames)

	log.Println("Check Snippet : ", data) //Snippet Validate Check
	resp := &types.QueryResponse{
		TableNames: data.TableNames,
		// TableNames: tblArr,
		// Field:      makeColumnToString(data.Parsedquery.Columns, data.TableSchema),
		// Values:     make([]map[string]string, 0),
		TableData: make(map[string]types.TableValues),
	}
	log.Println("Table Name >", resp.TableNames)
	log.Println("Block Offset >", data.BlockOffset)
	// log.Println("Real Path >", rootDirectory+data.TableNames[0]+".csv")
	// for _, name := range data.TableNames {
	// 	log.Println(rootDirectory + name + ".csv")
	// }
	log.Println("Scanning...")
	// fmt.Println(time.Now().Format(time.StampMilli), "Table Name >", resp.Table)
	// fmt.Println(time.Now().Format(time.StampMilli), "Block Offset >", data.BlockOffset)
	// fmt.Println(time.Now().Format(time.StampMilli), "Real Path >", rootDirectory+data.Parsedquery.TableName+".csv")
	// fmt.Println(time.Now().Format(time.StampMilli), "Scanning...")

	// CSV READ

	idx := -1
	for _, i := range data.CsdInfos.Items {
		for k, v := range i.Csd {
			if k == strconv.Itoa(myCsdNum) {
				idx = v
			}
		}
	}
	log.Println(idx)
	st = time.Now()
	for i := 0; i < len(data.TableNames); i++ {
		tableCSV, err := os.Open(rootDirectory + data.TableNames[i] + ".csv")
		// log.Println(rootDirectory + data.TableNames[i] + ".csv")
		if err != nil {
			//klog.Errorln(err)
			log.Println(err)
		}

		// csv reader 생성
		rdr := csv.NewReader(bufio.NewReader(tableCSV))

		// csv 내용 모두 읽기
		rows, _ := rdr.ReadAll()
		log.Println("Compleate Read", len(rows), "Data")
		// var totalLength int
		// for
		// fmt.Println(time.Now().Format(time.StampMilli), "Compleate Read", len(rows), "Data")

		csdTotalCount := data.CsdInfos.CsdTotal
		readLength := len(rows) / csdTotalCount
		log.Println("csdTotal:", csdTotalCount, " / readLength: ", readLength)
		// var startPoint int
		var endPoint int
		startPoint := readLength * idx
		if csdTotalCount == idx+1 {
			log.Println("Last CSD")
			endPoint = len(rows)
		} else {
			log.Println("Basic CSD")
			endPoint = startPoint + readLength
		}
		log.Println("Point: ", startPoint, endPoint)

		tableData := rowToTableData(rows, data.TableSchema[data.TableNames[i]], startPoint, endPoint)

		resp.TableData[data.TableNames[i]] = tableData
	}
	log.Println("CSV SCAN", time.Since(st).Seconds(), "SEC")
	log.Println("Send to Filtering Data...")

	// fmt.Println(time.Now().Format(time.StampMilli), "Send to Filtering Data...")

	log.Println("scandata make")
	filterBody := &ScanData{}
	filterBody.Snippet = *data
	filterBody.Tabledata = resp.TableData
	// log.Println("tableDataRaw: ", len(filterBody.Tabledata["lineitem"].Values["L_ORDERKEY"]))
	log.Println(filterBody.Snippet)
	log.Println(len(filterBody.Tabledata["lineitem"].Values))
	for key, _ := range filterBody.Tabledata {
		log.Println(key)
	}
	log.Println(filterBody.Snippet.WhereClauses)

	log.Println("marshall start")
	ss := time.Now()
	filterJson, err := jsoniter.Marshal(filterBody)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println(time.Since(ss).Seconds(), "SEC")
	// log.Println(string(filterJson))

	// res_byte, _ := json.MarshalIndent(filterBody, "", "  ")
	// fmt.Println("\n[ res_byte ]")
	// fmt.Println(string(res_byte))

	filterJson_buff := bytes.NewBuffer(filterJson)

	req, err := http.NewRequest("POST", "http://:8187", filterJson_buff)
	log.Println("TIME SCAN", time.Since(first).Seconds(), "SEC")
	if err != nil {
		log.Println("httperr : ", err)
	} else {

		client := &http.Client{}
		resp, errclient := client.Do(req)

		if errclient != nil {
			log.Println("resperr : ", errclient)
		} else {
			bytes, _ := ioutil.ReadAll(resp.Body)
			w.Write(bytes)
			defer resp.Body.Close()
		}
	}

}

func CSVParser(reqColumn []types.Select, schema types.TableSchema) []string {
	result := make([]string, 0)
	for _, sel := range reqColumn {
		if sel.ColumnType == 1 {
			if sel.ColumnName != "*" {
				result = append(result, sel.ColumnName)
			} else {
				result = append(result, schema.ColumnNames...)
			}
		}
	}
	return result
}

// CSV Data to Struct Data, 1106 update
func rowToTableData(rows [][]string, schema types.TableSchema, startPoint, endPoint int) types.TableValues {
	var tableData types.TableValues
	if startPoint == 0 {
		startPoint = 1
	}
	// for i := 1; i < len(rows); i++ {
	for i := startPoint; i < endPoint; i++ {
		// 인덱스 원소
		element := make(map[string]string)
		for j, col := range schema.ColumnNames {
			element[col] = rows[i][j]
		}
		tableData.Values = append(tableData.Values, element)
	}
	return tableData
	/*
		result := make(map[string][]string)
		for i := 0; i < len(schema.ColumnNames); i++ {
			result[rows[0][i]] = make([]string, 0)
			index := 0
			for {
				if schema.ColumnNames[index] == rows[0][i] {
					// log.Println(schema.ColumnNames[index])
					// log.Println(rows[0][i], i)
					break
				}
				index++
			}
			if startPoint == 0 {
				startPoint = 1
			}
			for j := 1; j < len(rows); j++ {
				// for j := startPoint; j < endPoint; j++ {
				result[rows[0][i]] = append(result[rows[0][i]], rows[j][i])
			}
			index = 0
			return types.TableValues{Values: result}
			}*/
}

func main() {
	log.SetFlags(log.Lshortfile)
	handler := http.NewServeMux()

	handler.HandleFunc("/", Scan)

	log.Println("Scan State [ Running ]")

	http.ListenAndServe(":8186", handler)
}
