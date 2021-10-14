package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
	"filter/type"
)

type ScanData struct {
	Snippet   types.Snippet       `json:"snippet"`
	Tabledata map[string][]string `json:"tabledata"`
}

type FilterData struct {
	Result   types.QueryResponse `json:"result"`
	TempData map[string][]string `json:"tempData"`
}

//where/column 필터링
func Filtering(w http.ResponseWriter, r *http.Request) {

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		//klog.Errorln(err)
	}

	recieveData := &ScanData{}
	err = json.Unmarshal(body, recieveData)
	if err != nil {
		//klog.Errorln(err)
	}

	data := recieveData.Snippet
	tableData := recieveData.Tabledata

	log.Println(tableData) //check where clause
	var tempData map[string][]string
	tempData = map[string][]string{}

	if len(data.Parsedquery.WhereClauses) == 0 {
		fmt.Println("Nothing to Filter")
		tempData = tableData
	} else {
		tempData = wherevalidator(data.Parsedquery.WhereClauses[0], data.TableSchema, tableData)
		if data.Parsedquery.WhereClauses[0].Operator != "NULL" {
			prevOerator := data.Parsedquery.WhereClauses[0].Operator
			wheres := data.Parsedquery.WhereClauses[1:]
			for i, where := range wheres {
				switch prevOerator {
				case "AND":
					tempData = wherevalidator(where, data.TableSchema, tempData)
				case "OR":
					tempData2 := wherevalidator(where, data.TableSchema, tableData)
					union := make(map[string][]string)
					for header, data := range tempData2 {
						union[header] = make([]string, 0)
						union[header] = append(union[header], data...)
						union[header] = append(union[header], tempData[header]...)
						union[header] = makeSliceUnique(union[header])
					}
					tempData = union
				}
				prevOerator = data.Parsedquery.WhereClauses[i].Operator
			}
		}
		rowCount := 0
		for header, _ := range tempData {
			if header != "" {
				rowCount = len(tempData[header])
				break
			}
		}
		fmt.Println(time.Now().Format(time.StampMilli), "Complete Filter", rowCount)
	}

	fmt.Println(time.Now().Format(time.StampMilli), "Send to Output Layer")

	resp := &types.QueryResponse{
		Table:         data.Parsedquery.TableName,
		BufferAddress: data.BufferAddress,
		Field:         makeColumnToString(data.Parsedquery.Columns, data.TableSchema),
		Values:        make([]map[string]string, 0),
	}

	outputBody := &FilterData{}
	outputBody.Result = *resp
	outputBody.TempData = tempData

	outputJson, err := json.Marshal(outputBody)
	outputJson_buff := bytes.NewBuffer(outputJson)

	outputJson_real_buff := outputJson_buff
	req, err := http.NewRequest("POST", "http://:3003", outputJson_real_buff)

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

func wherevalidator(where types.Where, schema types.TableSchema, currentMap map[string][]string) map[string][]string {
	resultIndex := make([]int, 0)
	columnIndex := foundIndex(schema.ColumnNames, where.LeftValue)
	if schema.ColumnTypes[columnIndex] == "int" {
		currentColumn := currentMap[where.LeftValue]
		rv, err := strconv.Atoi(where.RightValue)
		if err != nil {
			//klog.Errorln(err)
		}
		for i := 0; i < len(currentColumn); i++ {
			lv, err := strconv.Atoi(currentColumn[i])
			if err != nil {
				//klog.Errorln(err)
			}
			switch where.Exp {
			case "=":
				if lv == rv {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case ">=":
				if lv >= rv {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case "<=":
				if lv <= rv {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case ">":
				if lv > rv {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case "<":
				if lv < rv {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			}
		}
	} else if schema.ColumnTypes[columnIndex] == "date" {
		currentColumn := currentMap[where.LeftValue]
		where.RightValue = where.RightValue[1 : len(where.RightValue)-1]
		rv, err := time.Parse("2006-01-02", where.RightValue)
		if err != nil {
			//klog.Errorln(err)
		}
		for i := 0; i < len(currentColumn); i++ {
			lv, err := time.Parse("2006-01-02", currentColumn[i])
			if err != nil {
				//klog.Errorln(err)
			}
			switch where.Exp {
			case "=":
				if lv.Unix() == rv.Unix() {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case ">=":
				if lv.Unix() >= rv.Unix() {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case "<=":
				if lv.Unix() <= rv.Unix() {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case ">":
				if lv.Unix() > rv.Unix() {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}
			case "<":
				if lv.Unix() < rv.Unix() {
					resultIndex = append(resultIndex, i)
				} else {
					continue
				}

			}
		}
	} else {
		currentColumn := currentMap[where.LeftValue]
		for i := 0; i < len(currentColumn); i++ {
			if currentColumn[i] == where.RightValue {
				resultIndex = append(resultIndex, i)
			} else {
				continue
			}
		}
	}
	return rebuildMap(currentMap, resultIndex)
}

func makeSliceUnique(s []string) []string {
	keys := make(map[string]struct{})
	res := make([]string, 0)
	for _, val := range s {
		if _, ok := keys[val]; ok {
			continue
		} else {
			keys[val] = struct{}{}
			res = append(res, val)
		}
	}
	return res
}

func rebuildMap(currentMap map[string][]string, index []int) map[string][]string {
	resultMap := make(map[string][]string)
	for header, data := range currentMap {
		if header != "" {
			resultMap[header] = make([]string, 0)
			for i := 0; i < len(index); i++ {
				resultMap[header] = append(resultMap[header], data[index[i]])
			}
		}
	}

	return resultMap
}

func foundIndex(str []string, target string) int {
	index := -1
	for i := 0; i < len(str); i++ {
		if str[i] == target {
			index = i
			break
		}
	}
	return index
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

func main() {
	handler := http.NewServeMux()

	handler.HandleFunc("/", Filtering)

	log.Println("Filter State [ Running ]")

	http.ListenAndServe(":3002", handler)
}
