package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestGETDataHandler provides test of GET method for our service
func TestGETDataHandler(t *testing.T) {
	endPoints := []string{"/wmarchive/data", "/wmarchive/data/"}
	Config.Verbose = 1
	for _, endPoint := range endPoints {
		req, err := http.NewRequest("GET", endPoint, nil)
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(DataHandler)
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusOK)
		}

		expected := info()
		if rr.Body.String() != expected {
			t.Errorf("handler returned unexpected body: got %v want %v",
				rr.Body.String(), expected)
		}
	}
}

// TestPOSTDataHandler provides test of GET method for our service
func TestPOSTDataHandler(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	fname := fmt.Sprintf("%s/doc.json", dir)

	endPoints := []string{"/wmarchive/data", "/wmarchive/data/"}
	Config.Verbose = 1
	for _, endPoint := range endPoints {
		file, err := os.Open(fname)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		req, err := http.NewRequest("POST", endPoint, file)
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(DataHandler)
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusOK)
		}
		// {"result":[{"ids":["74193fb2a07d4302a1f14257454e4b53"],"status":"ok"}]}
		var rec map[string][]map[string]interface{}
		//     var rec TestRecord
		err = json.Unmarshal([]byte(rr.Body.String()), &rec)
		if err != nil {
			t.Fatal(err)
		}
		if vrec, ok := rec["result"]; ok {
			for _, r := range vrec {
				if v, ok := r["status"]; ok {
					if v.(string) != "ok" {
						t.Fatal("wmarchive record status is not ok")
					}
				} else {
					t.Fatal("wmarchive record does not have status field")
				}
			}
		} else {
			t.Fatal("unable to read wmarchive record")
		}
	}
}
