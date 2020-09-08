package main

// wmarchive - Go implementation of WMArchive service for CMS
//
// Copyright (c) 2020 - Valentin Kuznetsov <vkuznet@gmail.com>
//
// The WMArchive service accepts POST requests with data structure in the following way:
// {"data": [{data-record}, {data-record}, ...]}
// curl -X POST -H "Content-Type: application/json" -d@d.json <URL>/wmarchive/data
// Each data-record follows WMArchive schema defined at
// https://github.com/dmwm/WMArchive/blob/master/src/python/WMArchive/Schemas/FWJRProduction.py
// but for this service the structure of data-record is irrelevant (we only need to follow
// schema when we inject the data in ES).

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-stomp/stomp"
	"github.com/google/uuid"

	_ "expvar"         // to be used for monitoring, see https://github.com/divan/expvarmon
	_ "net/http/pprof" // profiler, see https://golang.org/pkg/net/http/pprof/

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
)

// global pointer to Stomp connection
var stompConn *stomp.Conn

// Configuration stores server configuration parameters
type Configuration struct {

	// HTTP server configuration options
	Port      int    `json:"port"`      // server port number
	Base      string `json:"base"`      // base URL
	Verbose   int    `json:"verbose"`   // verbose output
	Styles    string `json:"styles"`    // CSS styles path
	Jscripts  string `json:"js"`        // JS path
	Images    string `json:"images"`    // images path
	ServerCrt string `json:"serverCrt"` // path to server crt file
	ServerKey string `json:"serverKey"` // path to server key file
	LogFile   string `json:"logFile"`   // log file name

	// Stomp configuration options
	BufSize         int    `json:"bufSize"`         // buffer size
	StompURI        string `json:"stompURI"`        // StompAMQ URI
	StompLogin      string `json:"stompLogin"`      // StompAQM login name
	StompPassword   string `json:"stompPassword"`   // StompAQM password
	StompIterations int    `json:"stompIterations"` // Stomp iterations
	Endpoint        string `json:"endpoint"`        // StompAMQ endpoint
	ContentType     string `json:"contentType"`     // ContentType of UDP packet
}

// Config variable represents configuration object
var Config Configuration

// Record defines general WMArchive record
type Record map[string]interface{}

// custom rotate logger
type rotateLogWriter struct {
	RotateLogs *rotatelogs.RotateLogs
}

func (w rotateLogWriter) Write(data []byte) (int, error) {
	return w.RotateLogs.Write([]byte(utcMsg(data)))
}

// helper function to use proper UTC message in a logger
func utcMsg(data []byte) string {
	s := string(data)
	v, e := url.QueryUnescape(s)
	if e == nil {
		return v
	}
	return s
}

// helper function to parse configuration
func parseConfig(configFile string) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("Unable to read", err)
		return err
	}
	err = json.Unmarshal(data, &Config)
	if err != nil {
		log.Println("Unable to parse", err)
		return err
	}
	if Config.StompIterations == 0 {
		Config.StompIterations = 3 // number of Stomp attempts
	}
	if Config.ContentType == "" {
		Config.ContentType = "application/json"
	}
	return nil
}

// StompConnection returns Stomp connection
func StompConnection() (*stomp.Conn, error) {
	if Config.StompURI == "" {
		err := errors.New("Unable to connect to Stomp, not URI")
		return nil, err
	}
	if Config.StompLogin == "" {
		err := errors.New("Unable to connect to Stomp, not login")
		return nil, err
	}
	if Config.StompPassword == "" {
		err := errors.New("Unable to connect to Stomp, not password")
		return nil, err
	}
	conn, err := stomp.Dial("tcp",
		Config.StompURI,
		stomp.ConnOpt.Login(Config.StompLogin, Config.StompPassword))
	if err != nil {
		log.Printf("Unable to connect to %s, error %v", Config.StompURI, err)
	}
	if Config.Verbose > 0 {
		log.Printf("connected to StompAMQ server %s", Config.StompURI)
	}
	return conn, err
}

func sendDataToStomp(data []byte) error {
	var err error
	//     var stompConn *stomp.Conn
	for i := 0; i < Config.StompIterations; i++ {
		stompConn, err = StompConnection()
		if err != nil {
			log.Printf("Unable to get connection, %v", err)
			if stompConn != nil {
				stompConn.Disconnect()
			}
			continue
		}
		err = stompConn.Send(Config.Endpoint, Config.ContentType, data)
		if err != nil {
			if i == Config.StompIterations-1 {
				log.Printf("unable to send data to %s, error %v, iteration %d", Config.Endpoint, err, i)
			} else {
				log.Printf("unable to send data to %s, error %v, iteration %d", Config.Endpoint, err, i)
			}
			if stompConn != nil {
				stompConn.Disconnect()
			}
			//             stompConn, err = StompConnection()
		} else {
			if stompConn != nil {
				stompConn.Disconnect()
			}
			if Config.Verbose > 0 {
				log.Printf("send data to StompAMQ endpoint %s", Config.Endpoint)
			}
			return nil
		}
	}
	return err
}
func genUUID() string {
	uuidWithHyphen := uuid.New()
	uuid := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
	return uuid
}

func processRequest(r *http.Request) (Record, error) {
	var out []Record
	defer r.Body.Close()
	var rec Record
	// it is better to read whole body instead of using json decoder
	//     err := json.NewDecoder(r.Body).Decode(&rec)
	// since we can print body later for debugging purposes
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Unable to read request body", err)
	}
	err = json.Unmarshal(body, &rec)
	if err != nil {
		if Config.Verbose > 0 {
			log.Printf("Unable to decode input request, error %v, request %+v\n%+v\n", err, r, string(body))
		} else {
			log.Printf("Unable to decode input request, error %v\n", err)
		}
		return rec, err
	}
	var ids []string
	if v, ok := rec["data"]; ok {
		docs := v.([]interface{})
		for _, rrr := range docs {
			uid := genUUID()
			r := rrr.(map[string]interface{})
			if _, ok := r["wmats"]; !ok {
				r["wmats"] = time.Now().Unix()
			}
			if _, ok := r["wmaid"]; !ok {
				r["wmaid"] = uid
			}
			producer := "wmarchive"
			metadata := make(Record)
			//metadata["timestamp"] = time.Now().Unix() * 1000
			metadata["producer"] = producer
			metadata["_id"] = uid
			metadata["uuid"] = uid
			r["metadata"] = metadata
			data, err := json.Marshal(r)
			if err != nil {
				if Config.Verbose > 0 {
					log.Printf("Unable to marshal, error: %v, data: %+v\n", err, r)
				} else {
					log.Printf("Unable to marshal, error: %v, data\n", err)
				}
				continue
			}

			// dump message to our log
			if Config.Verbose > 1 {
				log.Println("New record", string(data))
			}

			// send data to Stomp endpoint
			if Config.Endpoint != "" {
				err := sendDataToStomp(data)
				if err == nil {
					ids = append(ids, uid)
				} else {
					record := make(Record)
					record["status"] = "fail"
					record["reason"] = fmt.Sprintf("Unable to send data to MONIT, error: %v", err)
					record["ids"] = ids
					out = append(out, record)
					r := make(Record)
					r["result"] = out
					return r, err
				}
			} else {
				ids = append(ids, uid)
			}
		}
	}

	record := make(Record)
	if len(ids) > 0 {
		record["status"] = "ok"
		record["ids"] = ids
	} else {
		record["status"] = "empty"
		record["ids"] = ids
		record["reason"] = "no input data is provided"
	}
	out = append(out, record)
	rec = make(Record)
	rec["result"] = out
	return rec, nil
}

func PostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	out, err := processRequest(r)
	if err != nil {
		log.Println(r.Method, r.URL.Path, r.RemoteAddr, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(out)
	var headers []interface{}
	headers = append(headers, r.Method)
	headers = append(headers, r.URL.Path)
	headers = append(headers, r.RemoteAddr)
	for _, h := range []string{"User-Agent", "Cms-Authn-Dn", "X-Forwarded-For"} {
		if v, ok := r.Header[h]; ok {
			headers = append(headers, v)
		}
	}
	if err == nil {
		if Config.Verbose > 0 {
			headers = append(headers, string(data))
		} else {
		}
		log.Println(headers...)
		w.WriteHeader(http.StatusOK)
		w.Write(data)
		return
	}
	headers = append(headers, err)
	log.Println(headers...)
	w.WriteHeader(http.StatusInternalServerError)
}

// http server implementation
func server(serverCrt, serverKey string) {
	// define server handlers
	base := Config.Base
	http.Handle(base+"/css/", http.StripPrefix(base+"/css/", http.FileServer(http.Dir(Config.Styles))))
	http.Handle(base+"/js/", http.StripPrefix(base+"/js/", http.FileServer(http.Dir(Config.Jscripts))))
	http.Handle(base+"/images/", http.StripPrefix(base+"/images/", http.FileServer(http.Dir(Config.Images))))
	// the request handler
	http.HandleFunc(fmt.Sprintf("%s", Config.Base), PostHandler)

	// start HTTP or HTTPs server based on provided configuration
	addr := fmt.Sprintf(":%d", Config.Port)
	if serverCrt != "" && serverKey != "" {
		//start HTTPS server which require user certificates
		server := &http.Server{Addr: addr}
		log.Printf("Starting HTTPs server on %s%s", addr, Config.Base)
		log.Fatal(server.ListenAndServeTLS(serverCrt, serverKey))
	} else {
		// Start server without user certificates
		log.Printf("Starting HTTP server on %s%s", addr, Config.Base)
		log.Fatal(http.ListenAndServe(addr, nil))
	}
}

// main function
func main() {
	var config string
	flag.StringVar(&config, "config", "", "configuration file")
	flag.Parse()
	err := parseConfig(config)
	if err != nil {
		log.Fatalf("Unable to parse config file %s, error: %v", config, err)
	}
	// set log file or log output
	if Config.LogFile != "" {
		logName := Config.LogFile + "-%Y%m%d"
		hostname, err := os.Hostname()
		if err == nil {
			logName = Config.LogFile + "-" + hostname + "-%Y%m%d"
		}
		rl, err := rotatelogs.New(logName)
		if err == nil {
			rotlogs := rotateLogWriter{RotateLogs: rl}
			log.SetOutput(rotlogs)
		} else {
			log.SetFlags(log.LstdFlags | log.Lshortfile)
		}
	} else {
		// log time, filename, and line number
		if Config.Verbose > 0 {
			log.SetFlags(log.LstdFlags | log.Lshortfile)
		} else {
			log.SetFlags(log.LstdFlags)
		}
	}

	_, e1 := os.Stat(Config.ServerCrt)
	_, e2 := os.Stat(Config.ServerKey)
	if e1 == nil && e2 == nil {
		server(Config.ServerCrt, Config.ServerKey)
	} else {
		server("", "")
	}
}
