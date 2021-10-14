package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

//스니펫 받아서 전달
func Input(w http.ResponseWriter, r *http.Request) {
	//data := []byte("-> Response From Server [10.0.6.132]")
	//w.Write(data)

	//body, _ := ioutil.ReadAll(r.Body)
	//bodyString := string(body)

	bodyString := getSnippet()
	log.Println()
	log.Println("Snippet >", bodyString)
	log.Println("Input Snippet ...")
	time.Sleep(time.Millisecond * 836)
	log.Println("Complete to Input Snippet")
	log.Println("Send Data to Scan Layer")

	input_buff := bytes.NewBuffer(body)

	req, err := http.NewRequest("POST", "http://:3001", input_buff)

	if err != nil {
		fmt.Println("httperr : ", err)
	} else {

		client := &http.Client{}
		resp, errclient := client.Do(req)

		if errclient != nil {
			fmt.Println("resperr : ", errclient)
		} else {
			bytes, _ := ioutil.ReadAll(resp.Body)
			w.Write(bytes)

			defer resp.Body.Close()
		}
	}

}

func getSnippet(w http.ResponseWriter, r *http.Request) {

	body, _ := ioutil.ReadAll(r.Body)
	bodyString := string(body)

	return bodyString
}

func main() {
	log.SetFlags(log.Lshortfile)
	handler := http.NewServeMux()

	handler.HandleFunc("/snippet", Input)

	log.Println("Input State [ Running ]...")

	// 2999는 기존 server
	http.ListenAndServe(":3000", handler)
	// 3000번은 simulator
	// http.ListenAndServe(":3000", handler)
}
