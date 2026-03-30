package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "Address of any cluster node")
	op := flag.String("op", "GET", "Operation: GET, PUT, DELETE")
	key := flag.String("key", "", "Key")
	val := flag.String("val", "", "Value (for PUT)")
	retries := flag.Int("retries", 5, "Number of retries")
	flag.Parse()

	if *key == "" {
		fmt.Println("Key is required")
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s/kv/%s", *addr, *key)

	for i := 0; i < *retries; i++ {
		var resp *http.Response
		var err error

		switch *op {
		case "GET":
			resp, err = http.Get(url)
		case "PUT":
			body, _ := json.Marshal(map[string]string{"Value": *val})
			resp, err = http.Post(url, "application/json", bytes.NewBuffer(body))
		case "DELETE":
			req, _ := http.NewRequest(http.MethodDelete, url, nil)
			resp, err = http.DefaultClient.Do(req)
		default:
			fmt.Printf("Unknown operation: %s\n", *op)
			os.Exit(1)
		}

		if err != nil {
			fmt.Printf("Attempt %d: Error: %v\n", i+1, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if resp.StatusCode == http.StatusTemporaryRedirect {
			newUrl := resp.Header.Get("Location")
			if newUrl != "" {
				fmt.Printf("Attempt %d: Redirecting to %s\n", i+1, newUrl)
				url = newUrl
				resp.Body.Close()
				continue
			}
		}

		if resp.StatusCode >= 500 {
			fmt.Printf("Attempt %d: Server error: %s. Retrying...\n", i+1, resp.Status)
			resp.Body.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		fmt.Printf("Response: [%s] %s\n", resp.Status, string(body))
		return
	}

	fmt.Println("Failed after multiple retries.")
	os.Exit(1)
}
