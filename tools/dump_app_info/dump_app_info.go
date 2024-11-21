package main

import (
	"fmt"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/splunknozzle"
	"github.com/cloudfoundry/go-cfclient/v3/client"
	"github.com/cloudfoundry/go-cfclient/v3/config"
	"os"
	"time"

	"github.com/cloudfoundry-community/splunk-firehose-nozzle/cache"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	skipSSL := kingpin.Flag("skip-ssl-validation", "Skip cert validation (for dev environments").
		Envar("SKIP_SSL_VALIDATION").Default("false").Bool()
	apiEndpoint := kingpin.Flag("api-endpoint", "API endpoint address").
		Envar("API_ENDPOINT").Required().String()
	user := kingpin.Flag("user", "Admin user.").
		Envar("API_USER").Required().String()
	password := kingpin.Flag("password", "Admin password.").
		Envar("API_PASSWORD").Required().String()
	boltdbPath := kingpin.Flag("boltdb-path", "Bolt Database path ").
		Default("appinfo-bolt.db").Envar("BOLTDB_PATH").String()
	kingpin.Parse()

	var skipSSLCF config.Option
	if *skipSSL {
		skipSSLCF = config.SkipTLSValidation()
	}
	cfConfig, err := config.New(*apiEndpoint, config.ClientCredentials(*user, *password), skipSSLCF, config.UserAgent("splunk-firehose-nozzle"))
	if err != nil {
		fmt.Printf("failed to create CF config, error=%+v\n", err)
		os.Exit(1)
	}

	cfClient, err := client.New(cfConfig)
	if err != nil {
		fmt.Printf("failed to create CF client, error=%+v\n", err)
		os.Exit(1)
	}

	nozzleCfClient := splunknozzle.NozzleCfClient(*cfClient)

	config := cache.BoltdbConfig{
		Path: *boltdbPath,
	}
	bolt, err := cache.NewBoltdb(nozzleCfClient, &config)
	if err != nil {
		fmt.Printf("failed to create boltdb caching client, error=%+v\n", err)
		os.Exit(1)
	}

	start := time.Now().Unix()
	fmt.Printf("Start populating boltdb=%s with app info\n", *boltdbPath)
	err = bolt.Open()
	if err != nil {
		fmt.Printf("failed to open boltdb caching, error=%+v\n", err)
		os.Exit(1)
	}

	err = bolt.Close()
	if err != nil {
		fmt.Printf("failed to populate boltdb caching, error=%+v\n", err)
		os.Exit(1)
	}

	end := time.Now().Unix()
	apps, err := bolt.GetAllApps()
	if err != nil {
		fmt.Printf("failed to get apps from boltdb caching, error=%+v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Finish populating boltdb=%s with %d app info, took=%d seconds\n", *boltdbPath, len(apps), end-start)
}
