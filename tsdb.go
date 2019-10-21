package main

import (
	"fmt"
	"math"
	"os"
	"flag"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
)

func main() {

	var logger log.Logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	dbpath := flag.String("dbpath", "/root/lm/bkup/data", "a string")
	newdbpath := flag.String("newdbpath", "/root/lm/bkup/newdata", "a string")
	mergedbpath := flag.String("mergedbpath", "/root/lm/bkup/mergedata", "a string")
	operation := flag.String("op", "dump", "a string")

	flag.Parse()

	isDump := true
        isCopy := false 
        isMerge := false 

	if strings.Compare(*operation, "copy") == 0 {
	    isDump = false
	    isCopy = true
   	} else if strings.Compare(*operation, "merge") == 0 {
            isDump = false
            isMerge = true
        }

	var reg = prometheus.NewRegistry()

	db, err := tsdb.Open(*dbpath, log.With(logger, "olddb", "tsdb"), reg, tsdb.DefaultOptions)

        if err != nil {
		fmt.Println("Open olddb error")
                fmt.Println(err.Error())
		return
        }

	if isDump == true {
	    dumpTsdb(db)

	    fmt.Println("finish dump tsdb ", *dbpath)
	}

        if isCopy == true {
	    var newreg = prometheus.NewRegistry()
	    option := tsdb.DefaultOptions

	    option.WALSegmentSize = 16*1024*1024 // 16 MB
	    newdb, err := tsdb.Open(*newdbpath, log.With(logger, "newdb", "tsdb"), newreg, option)

            if err != nil {
		fmt.Println("Open newdb error")
                fmt.Println(err.Error())
		return
            }

	    copyTsdb(db, newdb, true)

	    fmt.Println("finish data migration from ", *dbpath, " to ", *newdbpath)
	}

	if isMerge == true {
	    var newreg = prometheus.NewRegistry()
	    option := tsdb.DefaultOptions

	    newdb, err := tsdb.Open(*newdbpath, log.With(logger, "newdb", "tsdb"), newreg, option)

            if err != nil {
		fmt.Println("Open newdb error")
                fmt.Println(err.Error())
		return
            }

	    var mergereg = prometheus.NewRegistry()
	    mergedb, err := tsdb.Open(*mergedbpath, log.With(logger, "mergedb", "tsdb"), mergereg, option)

            if err != nil {
		fmt.Println("Open mergedb error")
                fmt.Println(err.Error())
		return
            }

	    mergeTsdb(db, newdb, mergedb, true)

	    fmt.Println("finish data merge from ", *dbpath, " and ", *newdbpath, " to ", *mergedbpath)
        }
}


func dumpTsdb(db *tsdb.DB){
	q, err := db.Querier(math.MinInt64, math.MaxInt64)
        if err != nil {
		fmt.Println("Querier error")
                fmt.Println(err.Error())
		return
        }

	labelValues , err := q.LabelValues("__name__")
	if err != nil {
               	fmt.Println("LabelValues error")
               	fmt.Println(err.Error())
               	return
       	}

	for i := 0; i < len(labelValues); i++ {
		matcher := labels.NewEqualMatcher("__name__", labelValues[i])
		set, err := q.Select(matcher)

        	if err != nil {
			fmt.Println("Select error")
                	fmt.Println(err.Error())
			return
        	}

		for set.Next() {
                	series := set.At()
			it := series.Iterator()
			labels := series.Labels()
			for it.Next() {
			    t, v := it.At()

			    fmt.Println(labels, t, v)
		         }
		}
	}
}

func copyTsdb(db *tsdb.DB, newdb *tsdb.DB, snapshot bool) {
	app := newdb.Appender()

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
        if err != nil {
		fmt.Println("Querier error")
                fmt.Println(err.Error())
		return
        }

	labelValues , err := q.LabelValues("__name__")
	if err != nil {
               	fmt.Println("LabelValues error")
               	fmt.Println(err.Error())
               	return
       	}

        //fmt.Println(labelValues)
	for i := 0; i < len(labelValues); i++ {
		matcher := labels.NewEqualMatcher("__name__", labelValues[i])
		set, err := q.Select(matcher)

        	if err != nil {
			fmt.Println("Select error")
                	fmt.Println(err.Error())
			return
        	}

		for set.Next() {
                	series := set.At()
			it := series.Iterator()
			labels := series.Labels()
			for it.Next() {
			    t, v := it.At()
			    fmt.Println(labels, t, v)

			    _, err := app.Add(labels, t, v)
			    if err != nil {
                        	fmt.Println("Add error")
                        	fmt.Println(err.Error())
                        	return
                	    }
		         }
		}
	}

	err = app.Commit()
	if err != nil {
           fmt.Println("Commit error")
           fmt.Println(err.Error())
       	}

        if snapshot == true {
          fmt.Println("snapshot db")
	  err = newdb.Snapshot("/tmp/snapshot/", false)
	  if err != nil {
            fmt.Println("Snapshot error")
            fmt.Println(err.Error())
       	  }
        }

/*
        fmt.Println("close db")
	err = db.Close()
	if err != nil {
           fmt.Println("Close old db error")
           fmt.Println(err.Error())
           return
       	}
*/

        fmt.Println("close new db")
	err = newdb.Close()
	if err != nil {
            fmt.Println("Close new db error")
            fmt.Println(err.Error())
            return
	}
}

func mergeTsdb(db *tsdb.DB, seconddb *tsdb.DB, mergedb *tsdb.DB, snapshot bool) {
	app := mergedb.Appender()

	q1, err := db.Querier(math.MinInt64, math.MaxInt64)
        if err != nil {
		fmt.Println("Querier error")
                fmt.Println(err.Error())
		return
        }

	q2, err := seconddb.Querier(math.MinInt64, math.MaxInt64)
        if err != nil {
		fmt.Println("Querier error")
                fmt.Println(err.Error())
		return
        }

	q_arr := []tsdb.Querier{q1, q2}

        for i := 0; i < 2; i++ {
	q := q_arr[i]
	labelValues , err := q.LabelValues("__name__")
	if err != nil {
               	fmt.Println("LabelValues error")
               	fmt.Println(err.Error())
               	return
       	}

        //fmt.Println(labelValues)
	for i := 0; i < len(labelValues); i++ {
		matcher := labels.NewEqualMatcher("__name__", labelValues[i])
		set, err := q.Select(matcher)

        	if err != nil {
			fmt.Println("Select error")
                	fmt.Println(err.Error())
			return
        	}

		for set.Next() {
                	series := set.At()
			it := series.Iterator()
			labels := series.Labels()
			for it.Next() {
			    t, v := it.At()
			    fmt.Println(labels, t, v)

			    _, err := app.Add(labels, t, v)
			    if err != nil {
                        	fmt.Println("Add error")
                        	fmt.Println(err.Error())
                        	return
                	    }
		         }
		}
	}
	}

	err = app.Commit()
	if err != nil {
           fmt.Println("Commit error")
           fmt.Println(err.Error())
       	}

        if snapshot == true {
          fmt.Println("snapshot db")
	  err = mergedb.Snapshot("/tmp/snapshot/", false)
	  if err != nil {
            fmt.Println("Snapshot error")
            fmt.Println(err.Error())
       	  }
        }

/*
        fmt.Println("close db")
	err = db.Close()
	if err != nil {
           fmt.Println("Close old db error")
           fmt.Println(err.Error())
           return
       	}
*/

        fmt.Println("close merge db")
	err = mergedb.Close()
	if err != nil {
            fmt.Println("Close merge db error")
            fmt.Println(err.Error())
            return
	}
}
