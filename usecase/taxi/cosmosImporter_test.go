package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestFetch(t *testing.T) {
	os.Setenv("AZURE_DATABASE", "xtoph-pilosa")
	os.Setenv("AZURE_DATABASE_PASSWORD", "UUEkv5WaMIytWXYiK6qZfnAPt4vwujN6f4PrsVZ08Dx4PQp0JYB1fcRjYZ4HWiLcDDcsPGzLj82laLFxXTEKng==")

	fmt.Println("fetch and parse")
	// setup channels

	//url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv"
	url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-02.csv"

	urls := make(chan string, 1)
	recs := make(chan Record, 1000)

	i := NewCosmosImporter()

	go func() {
		fmt.Printf("sending Url %s\n", url1)
		urls <- url1
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		i.fetch(urls, recs)
		wg.Done()
	}()

	wg.Wait()
	/*
		looking over that again, it may not be clear what my intentions were
		`fetch` and `parse` are intended to be called as groups of goroutines
		so that you can control the concurrency with which urls are being fetched
		the the parser routines read from the record channel, parse the fields and write to pilosa
	*/

	/*
		fmt.Println("fetch and parse")
		url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv"

		urls := make(chan string)
		recs := make(chan string)

		fetch(urls, recs)
		parse(recs)

		urls <- url1
	*/
}

func TestParse(t *testing.T) {
	/*
		looking over that again, it may not be clear what my intentions were
		`fetch` and `parse` are intended to be called as groups of goroutines
		so that you can control the concurrency with which urls are being fetched
		the the parser routines read from the record channel, parse the fields and write to pilosa
	*/

	/*
		fmt.Println("fetch and parse")
		url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv"

		urls := make(chan string)
		recs := make(chan string)

		fetch(urls, recs)
		parse(recs)

		urls <- url1
	*/
}
