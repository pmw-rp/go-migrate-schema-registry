package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"time"
)

type TopicSink struct {
	Path          string
	Seed          string      `koanf:"seed"`
	Topic         string      `koanf:"topic"`
	Compatibility string      `koanf:"compatibility"`
	TLS           *tls.Config `koanf:"tls"`
}

func (t *TopicSink) Connect() error {
	return nil
}

func (t *TopicSink) createSubjectSchemaRecord(value sr.SubjectSchema, deleted bool) (*kgo.Record, error) {
	key := make(map[string]interface{})

	key["keytype"] = "SCHEMA"
	key["subject"] = value.Subject
	key["version"] = value.Version
	key["magic"] = 1

	keyBytes, err := json.Marshal(key)
	if err != nil {
		return nil, fmt.Errorf("unable to marshall key into json: %v", key)
	}

	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("unable to marshall value into json: %v", value)
	}
	var v interface{}
	err = json.Unmarshal(valueBytes, &v)
	if err != nil {
		return nil, fmt.Errorf("unable to marshall value into json: %v", value)
	}
	vm := v.(map[string]interface{})
	vm["deleted"] = deleted
	//_, found := vm["references"]
	//if !found {
	//	vm["references"] = []string{}
	//}
	valueBytes, err = json.Marshal(v)

	record := &kgo.Record{
		Key:       keyBytes,
		Value:     valueBytes,
		Timestamp: time.Time{},
		Topic:     t.Topic,
		Partition: 0,
	}

	return record, nil
}

func (t *TopicSink) createCompatibilityRecord(value sr.CompatibilityResult) (*kgo.Record, error) {

	key := make(map[string]interface{})

	key["keytype"] = "CONFIG"
	key["magic"] = 0
	if value.Subject != "" {
		key["subject"] = value.Subject
	}

	keyBytes, err := json.Marshal(key)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal key into json: %v", key)
	}

	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal value into json: %v", value)
	}

	// TODO Looks like RP doesn't currently accept value keys other than compatibilityLevel
	var v interface{}
	err = json.Unmarshal(valueBytes, &v)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal value into json: %v", value)
	}
	vm := v.(map[string]interface{})
	// We delete unsupported keys
	delete(vm, "alias")
	delete(vm, "compatibilityGroup")
	delete(vm, "defaultMetadata")
	delete(vm, "defaultRuleSet")
	delete(vm, "overrideMetadata")
	delete(vm, "overrideRuleSet")
	delete(vm, "normalize")
	// Finally we ship the amended record
	valueBytes, err = json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal amended value into json: %v", value)
	}
	// End of bug handling

	record := &kgo.Record{
		Key:       keyBytes,
		Value:     valueBytes,
		Timestamp: time.Time{},
		Topic:     t.Topic,
		Partition: 0,
	}

	return record, nil
}

func (t *TopicSink) GetRecords(state *State) ([]*kgo.Record, error) {
	records := make([]*kgo.Record, 0)

	// Write opening compatibility level
	record := &kgo.Record{Topic: t.Topic, Key: []byte("{\"keytype\":\"CONFIG\",\"magic\":0}"), Value: []byte("{\"compatibilityLevel\":\"NONE\"}")}
	records = append(records, record)

	// Write subject versions
	for _, subjectSchema := range state.SubjectSchemas {
		ref := sr.SubjectVersion{Subject: subjectSchema.Subject, Version: subjectSchema.Version}

		// Find out whether this subject version has been deleted
		deleted := false
		for _, deletion := range state.SoftDeletions {
			if areEqual(ref, deletion) {
				deleted = true
				break
			}
		}

		// Write out record
		record, err := t.createSubjectSchemaRecord(subjectSchema, deleted)
		if err != nil {
			return nil, fmt.Errorf("unable to create subject schema record: %w", err)
		}
		records = append(records, record)
	}

	//Write subject compatibilities
	for _, result := range state.CompatibilityResults {
		record, err := t.createCompatibilityRecord(result)
		if err != nil {
			return nil, fmt.Errorf("unable to convert compatibility record into record: %w", err)
		}
		records = append(records, record)
	}

	// Write closing compatibility level
	record = &kgo.Record{Topic: t.Topic, Key: []byte("{\"keytype\":\"CONFIG\",\"magic\":0}"), Value: []byte(fmt.Sprintf("{\"compatibilityLevel\":\"%v\"}", t.Compatibility))}
	records = append(records, record)

	return records, nil
}

func (t *TopicSink) PutState(state *State) error {
	records, err := t.GetRecords(state)
	if err != nil {
		return fmt.Errorf("unable to convert state into records")
	}

	seeds := []string{t.Seed}
	opts := make([]kgo.Opt, 0)

	opts = append(opts, kgo.SeedBrokers(seeds...))

	if config.Exists(fmt.Sprintf("%v.sasl", t.Path)) {
		saslConfig := SASLConfig{}
		err = config.Unmarshal(fmt.Sprintf("%v.sasl", t.Path), &saslConfig)
		if err != nil {
			return fmt.Errorf("unable unmarshal SASL config: %w", err)
		}
		opts = SASLOpt(&saslConfig, opts)
	}

	if config.Exists(fmt.Sprintf("%v.tls", t.Path)) {
		tlsConfig := TLSConfig{}
		err = config.Unmarshal(fmt.Sprintf("%v.tls", t.Path), &tlsConfig)
		if err != nil {
			return fmt.Errorf("unable unmarshal TLS config: %w", err)
		}
		opts = TLSOpt(&tlsConfig, opts)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()

	for _, record := range records {
		if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
			fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
		}
	}

	return nil
}
