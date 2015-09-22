package db

import (
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/meta"
)

const (
	myHost = "localhost"
	myPort = 8086
)

// Client is a client.
type Client struct {
	*client.Client
}

// Open opens a connection to the database located at http://myHost:myPort using
// the environment variables INFLUX_USER and INFLUX_PWD, and returns the client for this DB.
func Open() *Client {
	u, err := url.Parse(fmt.Sprintf("http://%s:%d", myHost, myPort))
	if err != nil {
		log.Fatal(err)
	}

	conf := client.Config{
		URL:      *u,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PWD"),
	}

	con, err := client.NewClient(conf)
	if err != nil {
		log.Fatal(err)
	}

	_, _, err = con.Ping()
	if err != nil {
		log.Fatal(err)
	}

	cl := &Client{con}

	return cl
}

// QueryDB convenience function to query the database
func (cl *Client) QueryDB(cmd, db string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := cl.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	}
	return
}

// CreateDB creates a database within InfluxDB, ignoring the error if it already exists.
func (cl *Client) CreateDB(dbName string) {
	_, err := cl.QueryDB(fmt.Sprintf("create database \"%s\"", dbName), dbName)
	if err != nil {
		if err == meta.ErrDatabaseExists || err.Error() == "database already exists" {
			return
		}
		log.Fatal(err)
	}
}
