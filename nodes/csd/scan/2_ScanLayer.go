package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	types "scan/type"
)

const rootDirectory = "/home/ngd/workspace/usr/kch/ditributed/nodes/csd/data/"

// const rootDirectory = "/root/workspace/usr/coyg/module/tpch/"

type ScanData struct {
	Snippet   types.Snippet       `json:"snippet"`
	Tabledata map[string][]string `json:"tabledata"`
}

//데이터 파일 읽어옴
func Scan(w http.ResponseWriter, r *http.Request) {

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

	data := recieveData

	resp := &types.QueryResponse{
		Table:  data.Parsedquery.TableName,
		Field:  makeColumnToString(data.Parsedquery.Columns, data.TableSchema),
		Values: make([]map[string]string, 0),
	}
	log.Println("Table Name >", resp.Table)
	log.Println("Block Offset >", data.BlockOffset)
	log.Println("Real Path >", rootDirectory+data.Parsedquery.TableName+".csv")
	log.Println("Scanning...")
	// fmt.Println(time.Now().Format(time.StampMilli), "Table Name >", resp.Table)
	// fmt.Println(time.Now().Format(time.StampMilli), "Block Offset >", data.BlockOffset)
	// fmt.Println(time.Now().Format(time.StampMilli), "Real Path >", rootDirectory+data.Parsedquery.TableName+".csv")
	// fmt.Println(time.Now().Format(time.StampMilli), "Scanning...")
	tableCSV, err := os.Open(rootDirectory + data.Parsedquery.TableName + ".csv")
	if err != nil {
		//klog.Errorln(err)
		log.Println(err)
	}
	// csv reader 생성
	rdr := csv.NewReader(bufio.NewReader(tableCSV))

	// csv 내용 모두 읽기
	rows, _ := rdr.ReadAll()
	log.Println("Compleate Read", len(rows), "Data")
	// fmt.Println(time.Now().Format(time.StampMilli), "Compleate Read", len(rows), "Data")
	tableData := rowToTableData(rows, data.TableSchema)
	log.Println("Send to Filtering Data...")
	// fmt.Println(time.Now().Format(time.StampMilli), "Send to Filtering Data...")

	filterBody := &ScanData{}
	filterBody.Snippet = *data
	filterBody.Tabledata = tableData

	filterJson, err := json.Marshal(filterBody)
	if err != nil {
		fmt.Println(err)
		return
	}
	filterJson_buff := bytes.NewBuffer(filterJson)

	req, err := http.NewRequest("POST", "http://:8187", filterJson_buff)

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

func makeColumnToString(reqColumn []types.Select, schema types.TableSchema) []string {
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

func rowToTableData(rows [][]string, schema types.TableSchema) map[string][]string {
	result := make(map[string][]string)
	for i := 0; i < len(schema.ColumnNames); i++ {
		result[rows[0][i]] = make([]string, 0)
		index := 0
		for {
			if schema.ColumnNames[index] == rows[0][i] {
				break
			}
			index++
		}
		for j := 1; j < len(rows); j++ {
			result[rows[0][i]] = append(result[rows[0][i]], rows[j][i])
		}
		index = 0
	}
	return result
}

func main() {
	log.SetFlags(log.Lshortfile)
	handler := http.NewServeMux()

	handler.HandleFunc("/", Scan)

	log.Println("Scan State [ Running ]")

	http.ListenAndServe(":8186", handler)
}
