package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-stomp/stomp"
	"github.com/google/uuid"

	_ "expvar"         // to be used for monitoring, see https://github.com/divan/expvarmon
	_ "net/http/pprof" // profiler, see https://golang.org/pkg/net/http/pprof/
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

func processRequest(r *http.Request) ([]Record, error) {
	var out []Record
	defer r.Body.Close()
	var rec Record
	err := json.NewDecoder(r.Body).Decode(&rec)
	if err != nil {
		log.Println("Unable to decode input request", err)
		return out, err
	}
	var ids []string
	if v, ok := rec["data"]; ok {
		docs := v.([]interface{})
		for _, rrr := range docs {
			uid := genUUID()
			r := rrr.(map[string]interface{})
			producer := "wmarchive"
			metadata := make(Record)
			metadata["timestamp"] = time.Now().Unix() * 1000
			metadata["producer"] = producer
			metadata["_id"] = uid
			metadata["uuid"] = uid
			r["metadata"] = metadata
			data, err := json.Marshal(r)
			if err != nil {
				log.Println("Unable to marshal, error: %v", err)
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
					record["result"] = "fail"
					record["reason"] = fmt.Sprintf("Unable to send data to MONIT, error: %v", err)
					record["ids"] = ids
					out = append(out, record)
					return out, err
				}
			}
		}
	}

	record := make(Record)
	if len(ids) > 0 {
		record["result"] = "ok"
		record["ids"] = ids
	} else {
		record["result"] = "empty"
		record["ids"] = ids
		record["reason"] = "no input data is provided"
	}
	out = append(out, record)
	return out, nil
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
	// log time, filename, and line number
	if Config.Verbose > 0 {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	} else {
		log.SetFlags(log.LstdFlags)
	}

	_, e1 := os.Stat(Config.ServerCrt)
	_, e2 := os.Stat(Config.ServerKey)
	if e1 == nil && e2 == nil {
		server(Config.ServerCrt, Config.ServerKey)
	} else {
		server("", "")
	}
}
