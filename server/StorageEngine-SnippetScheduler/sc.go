package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    Data   `json:"data"`
}
type Data struct {
	// Table       string                         `json:"table"`
	Field  []string                       `json:"field"`
	Values map[string][]map[string]string `json:"values"`
}

var jsonDataString = []string{`{
				"code": 200,
				"message": "OK",
				"data": {
				  "table": "",
				  "field": [
					"L_ORDERKEY",
					"L_PARTKEY",
					"L_SUPPKEY",
					"L_LINENUMBER",
					"L_QUANTITY",
					"L_EXTENDEDPRICE",
					"L_DISCOUNT",
					"L_TAX",
					"L_RETURNFLAG",
					"L_LINESTATUS",
					"L_SHIPDATE",
					"L_COMMITDATE",
					"L_RECEIPTDATE",
					"L_SHIPINSTRUCT",
					"L_SHIPMODE",
					"L_COMMENT"
				  ],
				  "values": {
					"": [
					  {
						"L_ORDERKEY": "1",
						"L_PARTKEY": "15519",
						"L_SUPPKEY": "785",
						"L_LINENUMBER": "1",
						"L_QUANTITY": "17",
						"L_EXTENDEDPRICE": "24386.67",
						"L_DISCOUNT": "0.04",
						"L_TAX": "0.02",
						"L_RETURNFLAG": "A",
						"L_LINESTATUS": "B",
						"L_SHIPDATE": "1996.3.13",
						"L_COMMITDATE": "1996.2.12",
						"L_RECEIPTDATE": "1996.3.22",
						"L_SHIPINSTRUCT": "DELIVER IN PERSON",
						"L_SHIPMODE": "TRUCK",
						"L_COMMENT": "egular courts above the"
					  }					  
					]
				  }
				}
			  }`, `{
				"code": 200,
				"message": "OK",
				"data": {
				  "table": "",
				  "field": [
					"L_ORDERKEY",
					"L_PARTKEY",
					"L_SUPPKEY",
					"L_LINENUMBER",
					"L_QUANTITY",
					"L_EXTENDEDPRICE",
					"L_DISCOUNT",
					"L_TAX",
					"L_RETURNFLAG",
					"L_LINESTATUS",
					"L_SHIPDATE",
					"L_COMMITDATE",
					"L_RECEIPTDATE",
					"L_SHIPINSTRUCT",
					"L_SHIPMODE",
					"L_COMMENT"
				  ],
				  "values": {
					"": [
					  {
						"L_ORDERKEY": "2",
						"L_PARTKEY": "2",
						"L_SUPPKEY": "2",
						"L_LINENUMBER": "2",
						"L_QUANTITY": "2",
						"L_EXTENDEDPRICE": "24386.67",
						"L_DISCOUNT": "0.04",
						"L_TAX": "0.02",
						"L_RETURNFLAG": "C",
						"L_LINESTATUS": "D",
						"L_SHIPDATE": "1996.3.13",
						"L_COMMITDATE": "1996.2.12",
						"L_RECEIPTDATE": "1996.3.22",
						"L_SHIPINSTRUCT": "DELIVER IN PERSON",
						"L_SHIPMODE": "TRUCK",
						"L_COMMENT": "egular courts above the"
					  }					  
					]
				  }
				}
			  }`,
}

func combine(bytesList [][]byte) []byte {

	res := &Response{
		Data: Data{
			Values: make(map[string][]map[string]string),
		},
	}

	code := 200
	field := []string{}
	for _, bytes := range bytesList {
		startTime := time.Now()

		tmp := &Response{}
		err := json.Unmarshal(bytes, tmp)
		if err != nil {
			log.Println("err:", err)
		}
		endTime := time.Since(startTime).Seconds()
		fmt.Printf("Node Data Marshal: %0.1f sec\n", endTime)

		startTime = time.Now()
		if tmp.Code != 200 {
			code = tmp.Code
		}
		field = tmp.Data.Field

		res.Data.Values[""] = append(res.Data.Values[""], tmp.Data.Values[""]...)

		endTime = time.Since(startTime).Seconds()
		fmt.Printf("Append Node Data: %0.1f sec\n", endTime)

	}

	startTime := time.Now()

	res.Code = code
	res.Data.Field = field

	bytes, err3 := json.Marshal(res)
	if err3 != nil {

		log.Println("err3:", err3)
	}
	endTime := time.Since(startTime).Seconds()
	fmt.Printf("Combined Data Marshal: %0.1f sec\n", endTime)

	return bytes
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())
	nodes := []string{
		"http://10.0.5.101:8101",
		"http://10.0.5.102:8101",
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// log.Println()
		// log.Println("Scheduler Start")
		// log.Println("Started Health Check For: Node1(10.0.5.120:3100)")
		// log.Println("Node1(10.0.5.120:3100) Is Alive")
		// log.Println("Started Table Check For: Node1(10.0.5.120:3100)")
		// log.Println("Node1(10.0.5.120:3100) Exist Table")

		// log.Println("Started Health Check For: Node2(10.0.5.121:3100)")
		// log.Println("Node2(10.0.5.121:3100) Is Alive")
		// log.Println("Started Table Check For: Node2(10.0.5.121:3100)")
		// log.Println("Node2(10.0.5.121:3100) Exist Table")

		// log.Println("Routing Request Node1(10.0.5.120:3100)")
		// log.Println("Routing Request Node2(10.0.5.121:3100)")

		body, _ := ioutil.ReadAll(r.Body)
		// bodyString := string(body)

		bytesList := [][]byte{}
		var wg sync.WaitGroup
		wg.Add(len(nodes))

		startTime := time.Now()

		for _, node := range nodes {
			go func(node string) {
				defer wg.Done()

				input_buff := bytes.NewBuffer(body)
				// fmt.Println("input_buff:", input_buff)
				req, err := http.NewRequest("POST", node, input_buff)

				if err != nil {
					fmt.Println("httperr : ", err)
				} else {

					client := &http.Client{}
					resp, errclient := client.Do(req)
					//defer resp.Body.Close()

					if errclient != nil {
						fmt.Println("resperr : ", errclient)
					} else {
						bytes, _ := ioutil.ReadAll(resp.Body)

						//bytes = []byte(jsonDataString[i])
						bytesList = append(bytesList, bytes)

					}
				}
			}(node)

		}
		wg.Wait()

		endTime := time.Since(startTime).Seconds()
		fmt.Printf("Node Processing Complete: %0.1f sec\n", endTime)

		res_bytes := combine(bytesList)
		// fmt.Println(string(res_bytes))

		w.Write(res_bytes)

	})

	http.ListenAndServe(":8100", nil)
}
