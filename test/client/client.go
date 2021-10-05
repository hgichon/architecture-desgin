package main

import (
	// "bytes"
	// "encoding/json"
	// "errors"
	// "fmt"
	// "io/ioutil"
	// "log"
	"fmt"
	"io/ioutil"
	"net/http"
	// "os"
	// "strconv"
	// "strings"
	// "time"
)

func main() {
	SchedulerIP := "10.0.5.100"
	SchedulerPort := "8221"
	resp, err := http.Get("http://" + SchedulerIP + ":" + SchedulerPort)

	if err != nil {
		fmt.Println(err)
		return
	}
	robots, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	fmt.Printf("%s\n", robots)
}
