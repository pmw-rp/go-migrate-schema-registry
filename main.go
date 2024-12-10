package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/tlscfg"
	"os"

	"log"
	"maps"
	"slices"
	"strings"
)

var config = koanf.New(".")

type SASLConfig struct {
	Mechanism string `koanf:"mechanism"`
	Username  string `koanf:"username"`
	Password  string `koanf:"password"`
}

type TLSConfig struct {
	Enabled        bool   `koanf:"enabled"`
	ClientKeyFile  string `koanf:"client_key"`
	ClientCertFile string `koanf:"client_cert"`
	CaFile         string `koanf:"ca_cert"`
}

// SASLOpt Initializes the necessary SASL configuration options
func SASLOpt(config *SASLConfig, opts []kgo.Opt) []kgo.Opt {
	if config.Mechanism != "" ||
		config.Username != "" ||
		config.Password != "" {

		if config.Mechanism == "" ||
			config.Username == "" ||
			config.Password == "" {
			log.Fatalln("All of Mechanism, Username, Password " +
				"must be specified if any are")
		}
		mechanism := strings.ToLower(config.Mechanism)
		mechanism = strings.ReplaceAll(mechanism, "-", "")
		mechanism = strings.ReplaceAll(mechanism, "_", "")
		switch mechanism {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: config.Username,
				Pass: config.Password,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.Username,
				Pass: config.Password,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.Username,
				Pass: config.Password,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: config.Username,
				SecretKey: config.Password,
			}.AsManagedStreamingIAMMechanism()))
		default:
			log.Fatalf("Unrecognized sasl mechanism: %s", mechanism)
		}
	}
	return opts
}

func TLSOpt(tlsConfig *TLSConfig, opts []kgo.Opt) []kgo.Opt {
	if tlsConfig.Enabled {
		if tlsConfig.CaFile != "" ||
			tlsConfig.ClientCertFile != "" ||
			tlsConfig.ClientKeyFile != "" {
			tc, err := tlscfg.New(
				tlscfg.MaybeWithDiskCA(
					tlsConfig.CaFile, tlscfg.ForClient),
				tlscfg.MaybeWithDiskKeyPair(
					tlsConfig.ClientCertFile, tlsConfig.ClientKeyFile),
			)
			if err != nil {
				log.Fatalf("Unable to create TLS config: %v", err)
			}
			opts = append(opts, kgo.DialTLSConfig(tc))
		} else {
			opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
		}
	}
	return opts
}

func buildSource(path string) (Source, error) {
	sources := config.Get(path).(map[string]interface{})
	if len(sources) != 1 {
		return nil, fmt.Errorf("unable to build source - ")
	}
	sourceType := slices.Collect(maps.Keys(sources))[0]

	if sourceType == "rest" {
		source := RestSource{}
		err := config.Unmarshal(fmt.Sprintf("%v.rest", path), &source)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall rest source config")
		}
		source.Connect()
		return &source, nil
	}

	if sourceType == "file" {
		source := FileSource{}
		err := config.Unmarshal(fmt.Sprintf("%v.file", path), &source)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall file source config")
		}
		return &source, nil
	}

	if sourceType == "v1file" {
		source := FileSourceV1{}
		err := config.Unmarshal(fmt.Sprintf("%v.v1file", path), &source)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall v1file source config")
		}
		return &source, nil
	}

	panic(fmt.Errorf("unknown source type: %v", sourceType))
}

func buildSink(path string) (Sink, error) {
	sinks := config.Get(path).(map[string]interface{})
	if len(sinks) != 1 {
		return nil, fmt.Errorf("unable to build sink - too many")
	}
	sinkType := slices.Collect(maps.Keys(sinks))[0]

	//if sinkType == "rest" {
	//	config := RestSourceConfig{}
	//	err := config.Unmarshal("source.rest", &config)
	//	if err != nil {
	//		panic(fmt.Errorf("unable to unmarshall rest source config"))
	//	}
	//	source := RestSource{}
	//	source.Connect(config)
	//	return &source
	//}

	if sinkType == "file" {
		sink := FileSink{}
		err := config.Unmarshal(fmt.Sprintf("%v.file", path), &sink)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall rest source config")
		}
		return &sink, nil
	}

	if sinkType == "debug" {
		sink := DebugSink{}
		return &sink, nil
	}

	if sinkType == "topic" {
		sink := TopicSink{Path: fmt.Sprintf("%v.topic", path)}
		err := config.Unmarshal(fmt.Sprintf("%v.topic", path), &sink)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall topic sink config: %w", err)
		}
		err = sink.Connect()
		if err != nil {
			return nil, fmt.Errorf("unable to connect to topic sink: %w", err)
		}
		return &sink, nil
	}

	panic(fmt.Errorf("unknown sink type: %v", sinkType))
}

func areEqual(a, b sr.SubjectVersion) bool {
	return a.Subject == b.Subject && a.Version == b.Version
}

func main() {

	configFile := flag.String("config", "", "location of the config file to run")
	flag.Parse()
	if *configFile == "" {
		_, err := fmt.Printf("Usage of %s:\n", os.Args[0])
		if err != nil {
			return
		}
		flag.PrintDefaults()
		return
	}

	if err := config.Load(file.Provider(*configFile), yaml.Parser()); err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	action := config.Get("action")

	if action.(string) == "migrate" {

		source, err := buildSource("source")
		if err != nil {
			panic(err)
		}
		sink, err := buildSink("sink")
		if err != nil {
			panic(err)
		}

		state, err := source.GetState()
		if err != nil {
			panic(err)
		}

		state.sort()
		state.validate()

		err = sink.PutState(state)
		if err != nil {
			panic(err)
		}
	}

	if action.(string) == "validate" {
		sourceA, err := buildSource("sourceA")
		if err != nil {
			panic(err)
		}
		sourceB, err := buildSource("sourceA")
		if err != nil {
			panic(err)
		}

		stateA, err := sourceA.GetState()
		if err != nil {
			panic(err)
		}
		stateB, err := sourceB.GetState()
		if err != nil {
			panic(err)
		}

		stateA.sort()
		stateB.sort()

		stateA.validate()
		stateB.validate()

		validate(stateA, stateB)
	}
}
