package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
)

func call(method, base, key, value string) {
	endpoint := base + "/api/" + method

	var resp *http.Response
	var err error

	switch method {
	case "put":
		fmt.Printf("[client] PUT    key=%s value=%s → %s\n", key, value, base)
		resp, err = http.PostForm(endpoint, url.Values{"key": {key}, "value": {value}})
	case "get":
		fmt.Printf("[client] GET    key=%s → %s\n", key, base)
		resp, err = http.Get(endpoint + "?key=" + url.QueryEscape(key))
	case "delete":
		fmt.Printf("[client] DELETE key=%s → %s\n", key, base)
		req, _ := http.NewRequest(http.MethodDelete, endpoint+"?key="+url.QueryEscape(key), nil)
		resp, err = http.DefaultClient.Do(req)
	}

	if err != nil {
		log.Println(method, "error:", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("[client] RESPONSE: %s\n", body)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: demo http://node1:8080")
		os.Exit(1)
	}

	base := os.Args[1]

	call("put", base, "user:1", "Alice")
	call("put", base, "user:2", "Bob")
	call("put", base, "user:3", "Brioshe")
	call("put", base, "config:timeout", "30s")

	call("get", base, "user:1", "")
	call("get", base, "user:2", "")

	call("put", base, "user:1", "Alice Updated")
	call("get", base, "user:1", "")

	call("delete", base, "user:2", "")
	call("get", base, "user:2", "")

	const totalKeys = 100

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		call("put", base, key, val)
	}

	for _, i := range []int{0, totalKeys / 2, totalKeys - 1} {
		key := fmt.Sprintf("key-%d", i)
		call("get", base, key, "")
	}
}
