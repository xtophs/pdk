package main

import (
	"fmt"

	"github.com/Azure/go-autorest/autorest/utils"
)

// TaxiImporter imports NYC taxi ride data into cosmosdb
type TaxiImporter interface {
	fetch(urls <-chan string, records chan<- Record)
	parse(records <-chan Record)
}

// CosmosImporter struct
type CosmosImporter struct {
	database string
	password string
}

// NewCosmosImporter returns an initialized TaxiImporter interface
func NewCosmosImporter() TaxiImporter {
	return &CosmosImporter{
		database: utils.GetEnvVarOrExit("AZURE_DATABASE"),
		password: utils.GetEnvVarOrExit("AZURE_DATABASE_PASSWORD"),
	}
}

func (i *CosmosImporter) init() {
	i.database = utils.GetEnvVarOrExit("AZURE_DATABASE")
	i.password = utils.GetEnvVarOrExit("AZURE_DATABASE_PASSWORD")
}

func (i *CosmosImporter) fetch(urls <-chan string, records chan<- Record) {

	fmt.Println("CosmosImporter fetch")
	f := NewRecordManager()
	f.fetch(urls, records)
	return
}

func (i *CosmosImporter) parse(records <-chan Record) {
	return
}
