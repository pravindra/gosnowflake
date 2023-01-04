// Example: Fetch one row.
//
// No cancel is allowed as no context is specified in the method call Query(). If you want to capture Ctrl+C to cancel
// the query, specify the context and use QueryContext() instead. See selectmany for example.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
)

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (string, *sf.Config, error) {
	env := func(k string, failOnMissing bool) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable is not set.", k)
		}
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT", true)
	user := env("SNOWFLAKE_TEST_USER", true)
	password := env("SNOWFLAKE_TEST_PASSWORD", true)
	host := env("SNOWFLAKE_TEST_HOST", false)
	portStr := env("SNOWFLAKE_TEST_PORT", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)

	port := 443 // snowflake default port
	var err error
	if len(portStr) > 0 {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return "", nil, err
		}
	}

	falseValue := "false"
	params := make(map[string]*string)
	params["USE_CACHED_RESULT"] = &falseValue
	cfg := &sf.Config{
		Account:   account,
		User:      user,
		Password:  password,
		Host:      host,
		Port:      port,
		Protocol:  protocol,
		Warehouse: "COMPUTE_WH",
		Params:    params,
	}

	dsn, err := sf.DSN(cfg)
	return dsn, cfg, err
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx := context.Background()

	// Send half the queries to ROUTE_WH using a multi-statement and the other
	// half to the default warehouse (COMPUTE_WH)
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)

		var query string
		if i%2 == 0 {
			query = "use warehouse ROUTE_WH;select avg(l_linenumber) from snowflake_sample_data.tpch_sf1000.lineitem;use warehouse COMPUTE_WH"
		} else {
			query = "select sum(l_linenumber) from snowflake_sample_data.tpch_sf1000.lineitem"
		}
		go func() {
			defer wg.Done()
			runQuery(ctx, db, i, query)
		}()
		time.Sleep(time.Millisecond * 300)
	}
	wg.Wait()
}

func runQuery(ctx context.Context, db *sql.DB, idx int, queryText string) {
	ctx, _ = sf.WithMultiStatement(ctx, 0)
	rows, err := db.QueryContext(ctx, queryText) // no cancel is allowed
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", queryText, err)
	}
	defer rows.Close()
	//var v int
	numResultSets := 1
	cnt := 0
	for rows.Next() {
		cnt++
	}
	for rows.NextResultSet() {
		numResultSets++
		for rows.Next() {
			cnt++
		}
	}
	fmt.Printf("%v resultsets, %v rows\n", numResultSets, cnt)
	fmt.Printf("Congrats! You have successfully run job %v query %v with Snowflake DB!\n", idx, queryText)
}
