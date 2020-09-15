package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
)

// nats options
var natsOptions []nats.Option

// helper function to initialize NATS options
func initNATS() {
	// Connect Options.
	natsOptions = []nats.Option{nats.Name("WMArchive NATS Publisher")}

	// handle user certificates
	if Config.NatsKey != "" && Config.NatsCert != "" {
		natsOptions = append(natsOptions, nats.ClientCert(Config.NatsCert, Config.NatsKey))
	}
	// handle root CAs
	if len(Config.RootCAs) > 0 {
		for _, v := range Config.RootCAs {
			f := strings.Trim(v, " ")
			natsOptions = append(natsOptions, nats.RootCAs(f))
		}
	}

}

// helper function to publish NATS message
func publish(subj string, msg []byte) error {
	// Connect to NATS
	nc, err := nats.Connect(strings.Join(Config.NatsServers, ","), natsOptions...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	nc.Publish(subj, msg)
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Printf("NATS error: %v\n", err)
		return err
	} else {
		if Config.Verbose > 0 {
			log.Printf("Published on %s: '%s'\n", subj, msg)
		}
	}
	return nil
}

type NatsRecord struct {
	Task     string
	Campaign string
	Site     string
	Dataset  string
	ExitCode int
}

// helper function to prepare message for NATS
func prepare(rec Record) []NatsRecord {
	var out []NatsRecord
	var nrec NatsRecord
	if v, ok := rec["task"]; ok {
		nrec.Task = v.(string)
	}
	if v, ok := rec["Campaign"]; ok {
		nrec.Campaign = v.(string)
	}
	if rec["steps"] != nil {
		steps := rec["steps"].([]interface{})
		for _, step := range steps {
			r := step.(map[string]interface{})
			nr := NatsRecord{Task: nrec.Task, Campaign: nrec.Campaign}
			if s, ok := r["site"]; ok {
				nr.Site = s.(string)
			}
			errCount := 0
			if v, ok := r["errors"]; ok {
				errors := v.([]interface{})
				for _, e := range errors {
					fmt.Println("doc errors", e)
					if e == nil {
						continue
					}
					r := e.(map[string]interface{})
					if v, ok := r["exitCode"]; ok {
						nr.ExitCode = int(v.(float64))
						out = append(out, nr)
						errCount++
					}
				}
			}
			if errCount == 0 {
				out = append(out, nr)
			}
		}
	}
	return out
}
