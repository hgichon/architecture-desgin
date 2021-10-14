package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	types "output/type"
	"time"
)

type FilterData struct {
	Result   types.QueryResponse `json:"result"`
	TempData map[string][]string `json:"tempData"`
	Datacheck bool `json:"datacheck"`
}

type ResponseA struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	Datacheck bool `json:"datacheck"`
}

//결과 받아서 host서버에 전달
func Output(w http.ResponseWriter, r *http.Request) {
	//data := []byte("Response From Output Process")
	//w.Write(data)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		//klog.Errorln(err)
	}

	recieveData := &FilterData{}
	err = json.Unmarshal(body, recieveData)
	if err != nil {
		//klog.Errorln(err)
		fmt.Println(err)
	}

	result := &recieveData.Result
	tempData := recieveData.TempData

	tmp := makeResponse(result, tempData)

	// 0927 kjh update
	// endMeasureUrl := "http://localhost:50500/end/measure"
	// res, err := http.Get(endMeasureUrl)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(res)

	// body, _ := ioutil.ReadAll(res.Body)

	content, err := json.Marshal(tmp)

	if err != nil {
		abort(w, 500)
	} else {
		w.WriteHeader(tmp.Code)
		w.Write(content)
	}

}

func abort(rw http.ResponseWriter, statusCode int) {
	rw.WriteHeader(statusCode)
}

func makeResponse(resp *types.QueryResponse, resultData map[string][]string) ResponseA {
	fmt.Println(time.Now().Format(time.StampMilli), "Prepare Output Response...")
	maxLen := 0
	for _, header := range resp.Field {
		if maxLen < len(resultData[header]) {
			maxLen = len(resultData[header])
		}
	}
	for i := 0; i < maxLen; i++ {
		resultMap := make(map[string]string)
		for _, header := range resp.Field {
			if len(resultData[header]) > 1 {
				resultMap[header] = resultData[header][0]
				resultData[header] = resultData[header][1:]
			} else if len(resultData[header]) > 0 {
				resultMap[header] = resultData[header][0]
			} else {
				resultMap[header] = ""
			}
		}
		resp.Values = append(resp.Values, resultMap)
	}

	fmt.Println(time.Now().Format(time.StampMilli), "Buffer Address >", resp.BufferAddress)
	fmt.Println(time.Now().Format(time.StampMilli), "Complete To Prepare Response")
	fmt.Println(time.Now().Format(time.StampMilli), "Done")

	r := ResponseA{200, "OK", *resp}

	return r
}

func main() {
	log.SetFlags(log.Lshortfile)
	handler := http.NewServeMux()

	handler.HandleFunc("/", Output)

	log.Println("Output State [ Running ]")

	http.ListenAndServe(":3003", handler)
}
