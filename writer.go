package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type p2cWriter struct {
	conf          *config
	requests      chan *p2cRequest
	wg            sync.WaitGroup
	kafkaProducer sarama.AsyncProducer
	tx            prometheus.Counter
	ko            prometheus.Counter
}

func NewP2CWriter(conf *config, reqs chan *p2cRequest) (*p2cWriter, error) {
	var err error
	w := new(p2cWriter)
	w.conf = conf
	w.requests = reqs
	w.kafkaProducer, err = newKafka(conf)
	if err != nil {
		fmt.Printf("Error connecting to kafka: %s\n", err.Error())
		return w, err
	}

	w.tx = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
	)

	w.ko = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
	)

	prometheus.MustRegister(w.tx)
	prometheus.MustRegister(w.ko)

	return w, nil
}

func newKafka(conf *config) (sarama.AsyncProducer, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	kafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Retry.Max = 3
	adders := strings.Split(conf.KafkaAddress, ",")
	producer, err := sarama.NewAsyncProducer(adders, kafkaConfig)
	return producer, err
}

type p2cRequestJson struct {
	Name string      `json:"name"`
	Tags []string    `json:"tags"`
	Val  interface{} `json:"val"`
	Ts   int64       `json:"ts"`
}

func send(w *p2cWriter, producer sarama.AsyncProducer) {
	w.wg.Add(1)
	fmt.Println("Writer starting..")
	ok := true
	for ok {
		// get next batch of requests
		var req *p2cRequest
		req, ok = <-w.requests
		if !ok {
			fmt.Printf("receive chan error, break.\n")
			break
		}
		reqJson := new(p2cRequestJson)
		reqJson.Name = req.name
		reqJson.Tags = req.tags
		if math.IsNaN(req.val) {
			reqJson.Val = "NaN"
		} else if math.IsInf(req.val, 1) {
			reqJson.Val = "Inf"
		} else if math.IsInf(req.val, -1) {
			reqJson.Val = "-Inf"
		} else {
			reqJson.Val = req.val
		}
		reqJson.Ts = req.ts.Unix()
		jsonMarshal, jsonErr := json.Marshal(&reqJson)
		if jsonErr != nil {
			fmt.Printf("Error Marshal json, name: %s, val: %f,  error: %s\n", reqJson.Name, req.val, jsonErr.Error())
			continue
		}
		//fmt.Printf("json: %s\n", jsonMarshal)
		msg := sarama.ProducerMessage{
			Topic: w.conf.KafkaTopic,
			Value: sarama.ByteEncoder(jsonMarshal),
		}

		producer.Input() <- &msg
	}
	defer producer.AsyncClose()
	fmt.Println("Writer stopped..")
	w.wg.Done()
}

func (w *p2cWriter) Start() {
	producer := w.kafkaProducer

	go func() {
		for true {
			select {
			case <-producer.Successes():
				w.tx.Add(1.0)
				//fmt.Printf("offset: %d,  timestamp: %s", suc.Offset, suc.Timestamp.String())
			case fail := <-producer.Errors():
				w.ko.Add(1.0)
				fmt.Printf("producer err: %s\n", fail.Err.Error())
			}
		}
	}()

	for i := 0; i < 5; i++ {
		go send(w, producer)
	}
}

func (w *p2cWriter) Wait() {
	w.wg.Wait()
}
