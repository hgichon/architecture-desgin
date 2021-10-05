package main

import (
	"fmt"
	"net/http"
	"strconv"
)

func main() {
	http.Handle("/", new(testHandler))

	http.ListenAndServe(":8221", nil)
}

type testHandler struct {
	http.Handler
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	str := "Your Request Path is " + req.URL.Path
	for i := 0; i < 100000; i++ {
		fmt.Println(req.RemoteAddr + " loop" + strconv.Itoa(i))

	}
	w.Write([]byte(str))
}
