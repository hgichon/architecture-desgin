package main

import (
	"bytes"
	"encoding/json"
	types "filter/type"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ScanData struct {
	Snippet   types.Snippet                `json:"snippet"`
	Tabledata map[string]types.TableValues `json:"tabledata"`
}

type FilterData struct {
	Result   types.QueryResponse          `json:"result"`
	TempData map[string]types.TableValues `json:"tempData"`
}

//where/column 필터링
func Filtering(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		//klog.Errorln(err)
		log.Println(err)
	}
	log.Println("body read")

	recieveData := &ScanData{}
	err = json.Unmarshal(body, recieveData)
	if err != nil {
		//klog.Errorln(err)
		log.Println(err)
	}
	log.Println("marshalling end")

	data := recieveData.Snippet
	tableData := recieveData.Tabledata


	var tempData map[string]types.TableValues
	// tempData = map[string][]string{}


	if len(data.WhereClauses) == 0 {
		fmt.Println("Nothing to Filter")
		tempData = tableData
	} else {
<<<<<<< HEAD
		log.Println("checking where")
		// log.Println(tableData["lineitem"].Values["L_SHIPDATE"][0])
		tempData = checkWhere(data.WhereClauses[0], data.TableSchema, tableData, data.TableNames)
		// log.Println(tempData)
		if data.WhereClauses[0].Operator != "NULL" {
			prevOerator := data.WhereClauses[0].Operator
			wheres := data.WhereClauses[1:]
			for i, where := range wheres {
				switch prevOerator {
				case "AND":
					tempData = checkWhere(where, data.TableSchema, tempData, data.TableNames)
				case "OR":
					// 생각 필요
					// tempData2 := checkWhere(where, data.TableSchema, tableData, data.TableNames)
					// union := make(map[string][]string)
					// for header, data := range tempData2 {
					// 	union[header] = make([]string, 0)
					// 	union[header] = append(union[header], data...)
					// 	union[header] = append(union[header], tempData[header]...)
					// 	union[header] = makeSliceUnique(union[header])
					// }
					// tempData = union
				}
				prevOerator = data.WhereClauses[i].Operator
			}
		}
		rowCount := 0
		for tname, values := range tempData {
			for cname, _ := range values.Values {
				if cname != "" {
					rowCount = len(values.Values[cname])
					break
				}
			}
			log.Println(time.Now().Format(time.StampMilli), "Table Name: ", tname, "Complete Filter", rowCount)

		}
	}

	fmt.Println(time.Now().Format(time.StampMilli), "Send to Output Layer")
	var fields []string
	// var values map[string][]map[string]string

	// tempData
	log.Println(len(tempData["lineitem"].Values["L_SHIPDATE"]))
	// var value []map[string]string
	// feild
	for _, values := range tempData {
		for col, _ := range values.Values {
			if col == "lineitem" {
				continue
			}
			fields = append(fields, col)
		}
	}
	log.Println(fields)

	// log.Println(data.TableNames[0])
	// values
	values := map[string][]map[string]string{}
	var arr []map[string]string
	val := map[string]string{}
	key := "none-group"
	idx := 0
	firstTbl := data.TableNames[0]
	colName := data.TableSchema[firstTbl].ColumnNames[0]
	log.Println(firstTbl, colName)
	log.Println(len(tempData[firstTbl].Values[colName]))
	ll := tempData[firstTbl].Values[strings.ToUpper(data.TableSchema[firstTbl].ColumnNames[0])]
	log.Println(len(ll))
	for i := 0; i < len(ll); i++ {
		for _, tt := range tempData {
			// 한 테이블 데이터
			tblData := tt.Values
			// for i := 0; i < len(tblData[fields[0]]); i++ {
			for col, d := range tblData {
				if col == "lineitem" {
					continue
				}
				val[col] = d[idx]
				// log.Println(col)
			}
		}
		idx++
		if idx < 3 {
			log.Println(idx, val)
			log.Println(arr)
		}
		arr = append(arr, val)
		val = map[string]string{}
	}
	values[key] = arr
	log.Println()
	// log.Println(values[key])

	log.Println(values[key][0])
	log.Println(len(values[key]))

	resp := &types.QueryResponse{
		Table: data.TableNames,
		// BufferAddress: data.BufferAddress,
		Field:  fields,
		Values: values,
		// TableData: tempData,
	}

	outputBody := &FilterData{}
	outputBody.Result = *resp
	outputBody.TempData = tempData

	log.Println("marshalling start")
	outputJson, err := json.Marshal(outputBody)
	if err != nil {
		log.Println(err)
	}
	log.Println("endeeddd")
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

<<<<<<< HEAD
func checkWhere(where types.Where, schema map[string]types.TableSchema, tableData map[string]types.TableValues, tableNames []string) map[string]types.TableValues {
	log.Println("checkwhere func..")
	// TODO: 수정 필요
	tblSchema := schema[tableNames[0]]
	tblData := tableData[tableNames[0]]
	for i, _ := range tblData.Values {
		log.Println(i)
	}
	// log.Println(tblSchema)
	// log.Println(tblData)
=======
func wherevalidator(where types.Where, schema types.TableSchema, currentMap map[string][]string) map[string][]string {
>>>>>>> bd950cde622f4094dc536d41186209ca20eadff0
	resultIndex := make([]int, 0)
	whereCmd := rvCheck(where)
	log.Println(whereCmd)
	// 컬럼 인덱스 찾기
	columnIndex := foundIndex(tblSchema.ColumnNames, where.LeftValue)
	log.Println("found index complete")
	log.Println("columnIndex", columnIndex)
	if tblSchema.ColumnTypes[columnIndex] == "int" {
		log.Println("int")
		// 현재 컬럼의 데이터들
		currentColumn := tblData.Values[where.LeftValue]
		rv, err := strconv.Atoi(where.RightValue)
		if err != nil {
			//klog.Errorln(err)
			log.Println(err)
		}

		for i := 0; i < len(currentColumn); i++ {
			log.Println("index", i)
			lv, err := strconv.Atoi(currentColumn[i])
			if err != nil {
				//klog.Errorln(err)
				log.Println(err)
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
	} else if tblSchema.ColumnTypes[columnIndex] == "date" {
		// TODO: lvcheck 필요
		currentColumn := tblData.Values[strings.ToUpper(where.LeftValue)]
		// log.Println(currentColumn)
		// whereCmd := rvCheck(where)
		log.Println(whereCmd)
		rv := dateWhere(whereCmd)
		log.Println(rv)
		// where.RightValue = where.RightValue[1 : len(where.RightValue)-1]

		// rv, err := time.Parse("2006-01-02", where.RightValue)
		// if err != nil {
		// 	//klog.Errorln(err)
		// 	log.Println(err)
		// }
		log.Println(len(currentColumn))
		// cpuCnt := runtime.NumCPU()
		// subSize := len(currentColumn) / cpuCnt
		// start := 0
		// end := subSize
		var wait sync.WaitGroup
		wait.Add(len(currentColumn))
		c := make(chan int, len(currentColumn))
		// for j := 0; j < subSize+1; j++ {
		// defer wait.Done()
		// var subResultIndex []int
		log.Println("len", len(currentColumn))
		for i := 0; i < len(currentColumn); i++ {
			go func(i int, c chan int) {
				// log.Println("i:", i)
				defer wait.Done()
				// wait.Add(1)
				changeCol := strings.Replace(currentColumn[i], ".", "-", -1)
				changeColList := strings.Split(changeCol, "-")
				for idx, j := range changeColList {
					if len(j) == 1 {
						changeColList[idx] = "0" + j
					}
				}
				changeCol = strings.Join(changeColList, "-")
				// log.Println(changeCol)
				lv, err := time.Parse("2006-01-02", changeCol)
				if err != nil {
					//klog.Errorln(err)
					log.Println(err)
				}
				// log.Println(lv)
				switch where.Exp {
				case "=":
					if lv.Unix() == rv.Unix() {
						// subResultIndex = append(subResultIndex, i)
						c <- i
					}
					// else {
					// 	continue
					// }
				case ">=":
					if lv.Unix() >= rv.Unix() {
						// subResultIndex = append(subResultIndex, i)
						c <- i
					}
					// else {
					// 	continue
					// }
				case "<=":
					if lv.Unix() <= rv.Unix() {
						// subResultIndex = append(subResultIndex, i)
						c <- i
					}
					// else {
					// 	continue
					// }
				case ">":
					if lv.Unix() > rv.Unix() {
						// subResultIndex = append(subResultIndex, i)
						c <- i
					}
					// else {
					// 	continue
					// }
				case "<":
					if lv.Unix() < rv.Unix() {
						// subResultIndex = append(subResultIndex, i)
						c <- i
					}
					// else {
					// 	continue
					// }

				}
				// c <- subResultIndex
			}(i, c)
		}
		// start += subSize
		// if end+subSize > len(currentColumn) {
		// 	end = len(currentColumn)
		// } else {
		// 	end += subSize
		// }
		// }
		wait.Wait()
		log.Println("asdfsdf")
		// sum := 0
		for i := 0; i < len(c); i++ {
			sub := <-c
			// log.Println(i, len(subs))
			// sum += len(subs)
			resultIndex = append(resultIndex, sub)
		}
		sort.Ints(resultIndex)
		// log.Println(sum)
		log.Println(len(resultIndex))
	} else {
		currentColumn := tblData.Values[where.LeftValue]
		for i := 0; i < len(currentColumn); i++ {
			if currentColumn[i] == where.RightValue {
				resultIndex = append(resultIndex, i)
			} else {
				continue
			}
		}
	}
	changeData := rebuildMap(tableData, resultIndex, tableNames)
	newTableValues := types.TableValues{
		Values: changeData,
	}
	tableData[tableNames[0]] = newTableValues
	return tableData
}

func rvCheck(where types.Where) []string {
	rv := where.RightValue
	log.Println("right value:", rv)
	slice := strings.Split(rv, " ")
	// sidx := -1
	joinFlag := false

	var whereCmd []string
	var tmpCmd []string
	// 괄호 합치기
	for _, s := range slice {
		if !joinFlag {
			if strings.Contains(s, "(") {
				joinFlag = true
				tmpCmd = append(tmpCmd, s)
			} else {
				whereCmd = append(whereCmd, s)
			}
		} else {
			if strings.Contains(s, ")") {
				joinFlag = false
				tmpCmd = append(tmpCmd, s)
				tmpStr := strings.Join(tmpCmd, "")
				log.Println(tmpStr)
				whereCmd = append(whereCmd, tmpStr)
				tmpCmd = []string{}
			} else {
				tmpCmd = append(tmpCmd, s)
			}
		}
	}
	for _, i := range whereCmd {
		log.Println(i)
	}

	return whereCmd
}

func dateWhere(whereCmd []string) time.Time {
	var cmdArr []interface{}
	prevCmd := ""
	for i := 0; i < len(whereCmd); i++ {
		cuCmd := whereCmd[i]
		if strings.Contains(cuCmd, "'") {
			tmp := strings.Replace(cuCmd, "'", "", -1)
			whereCmd[i] = tmp
		}
		// log.Println(whereCmd[i])
	}
	log.Println(whereCmd)
	// flag := false
	var firstDate time.Time
	var cal string
	var res time.Time
	for i := 0; i < len(whereCmd); i++ {
		cuCmd := whereCmd[i]
		// left
		if prevCmd == "" {
			if cuCmd == "date" {
				prevCmd = cuCmd
				log.Println("date matching")
			}
		} else {
			if prevCmd == "date" {
				dateParse, err := time.Parse("2006-01-02", cuCmd)
				if err != nil {
					log.Println(err)
				}
				log.Println(dateParse)
				cmdArr = append(cmdArr, dateParse)
				prevCmd = dateParse.String()
				firstDate = dateParse
			}
			if cuCmd == "-" {
				cmdArr = append(cmdArr, cuCmd)
				cal = cuCmd
				prevCmd = cuCmd
			} else if cuCmd == "interval" {
				numberCmd := whereCmd[i+1]
				num, err := strconv.Atoi(numberCmd)
				if err != nil {
					log.Println(err)
				}

				mdyCmd := whereCmd[i+2]
				log.Println(num, mdyCmd)
				if mdyCmd == "day" {
					if cal == "-" {
						num = -num
						log.Println(num)
						res = firstDate.AddDate(0, 0, num)
					}
				}
			}
		}

	}
	log.Println(cmdArr...)
	log.Println(res, reflect.TypeOf(res))

	return res
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

func rebuildMap(currentMap map[string]types.TableValues, index []int, tableNames []string) map[string][]string {
	resultMap := make(map[string][]string)
	tblValues := currentMap[tableNames[0]].Values
	// cname, cdata
	for header, data := range tblValues {
		if header != "" {
			resultMap[tableNames[0]] = make([]string, 0)
			for i := 0; i < len(index); i++ {
				resultMap[header] = append(resultMap[header], data[index[i]])
			}
		}
	}

	return resultMap
}

func foundIndex(tblSchemaColumnName []string, leftValue string) int {
	log.Println(tblSchemaColumnName, leftValue)
	index := -1
	for i := 0; i < len(tblSchemaColumnName); i++ {
		log.Println(tblSchemaColumnName[i])
		if strings.ToLower(tblSchemaColumnName[i]) == leftValue {
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
	log.SetFlags(log.Lshortfile)
	runtime.GOMAXPROCS(runtime.NumCPU())
	handler := http.NewServeMux()

	handler.HandleFunc("/", Filtering)

	log.Println("Filter State [ Running ]")

	http.ListenAndServe(":3002", handler)
}
