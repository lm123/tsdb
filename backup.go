package main

import (
	"fmt"
	_ "math"
	"os"
	"flag"
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

func main() {

	var logger log.Logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

        url := flag.String("url", "http://localhost:19090/", "a string")

        tsdbpath := flag.String("tsdb", "/tmp/data/", "a string")

  	flag.Parse()

        config := api.Config{
                Address: *url,
        }

        // Create new client.
        c, err := api.NewClient(config)
        if err != nil {
           fmt.Fprintln(os.Stderr, "error creating API client:", err)
	   return
        }

	// Run query against client.
        api := v1.NewAPI(c)

        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

        // query labeNames
        labelNames,err := api.LabelValues(ctx, "__name__")
        if err != nil {
                fmt.Fprintln(os.Stderr, "query LabelNames  error:", err)
                return
        }

	// create tsdb
	var reg = prometheus.NewRegistry()
	newdb, err := tsdb.Open(*tsdbpath, log.With(logger, "newdb", "tsdb"), reg, tsdb.DefaultOptions)

        if err != nil {
		fmt.Println("Open newdb error")
                fmt.Println(err.Error())
		return
        }

	app := newdb.Appender()

	minTime := time.Now().Add(-2*24 * time.Hour)
        maxTime := time.Now()

	for iter := 0; iter < 14; iter++ {

            //resolution := math.Max(math.Floor(maxTime.Sub(minTime).Seconds()/250), 1)
            //step := time.Duration(resolution) * time.Second
            step := 20 * time.Second

	    r := v1.Range{Start: minTime, End: maxTime, Step: step}

	    for i := 0; i < len(labelNames); i++ {

		query := labelNames[i]

		value, _, err := api.QueryRange(ctx, string(query), r)

        	if err != nil {
			fmt.Println("Series error")
                	fmt.Println(err.Error())
			return
        	}

		vt := value.Type()
	 	if vt == model.ValMatrix {	
		    var m model.Matrix = value.(model.Matrix)
		    for j := 0; j < len(m); j++ {
			ss := *m[j]
			metric := ss.Metric
			sp := ss.Values
			
			var lbs []labels.Label
			for name,val := range metric {
			    var label labels.Label

			    label.Name  = string(name)
			    label.Value = string(val)

			    lbs = append(lbs, label)
			}

			for k := 0; k < len(sp); k++ {

			    t := sp[k].Timestamp
			    v := sp[k].Value

			    fmt.Println(lbs, t, v)

			    _, err := app.Add(lbs, int64(t), float64(v))
			    if err != nil {
                        	fmt.Println("Add error")
                        	fmt.Println(err.Error())
                        	return
                	    }
			}
		    }
		} else if vt == model.ValVector {
		    var m model.Vector = value.(model.Vector)

		    for j := 0; j < len(m); j++ {
			sample := *m[j]
			metric := sample.Metric

			var lbs []labels.Label
			for name,val := range metric {
			    var label labels.Label

			    label.Name  = string(name)
			    label.Value = string(val)

			    lbs = append(lbs, label)
			}

			_, err := app.Add(lbs, int64(sample.Timestamp), float64(sample.Value))
			if err != nil {
                            fmt.Println("Add error")
                            fmt.Println(err.Error())
                            return
                	}
		    }
		} else if vt == model.ValScalar {
		    var m model.Scalar = *(value.(*model.Scalar))

		    var lbs []labels.Label

	  	    _, err := app.Add(lbs, int64(m.Timestamp), float64(m.Value))
		    if err != nil {
                        fmt.Println("Add error")
                        fmt.Println(err.Error())
                        return
                    }
		} else if vt == model.ValString {
		    var m model.String = *(value.(*model.String))

		    var lbs []labels.Label

		    fmt.Println("ValString:",m, lbs)

		    /*
	  	    _, err := app.Add(lbs, int64(m.Timestamp), m.Value)
		    if err != nil {
                        fmt.Println("Add error")
                        fmt.Println(err.Error())
                        return
                    }*/
		}
	    }
	    maxTime = minTime
    	    minTime = maxTime.Add(-2*24*time.Hour)
	}

	cancel()

	err = app.Commit()
	if err != nil {
               	fmt.Println("Commit error")
               	fmt.Println(err.Error())
               	return
       	}


	err = newdb.Snapshot("/tmp/snapshot/",true)
	if err != nil {
               	fmt.Println("tsdb snapshot error")
               	fmt.Println(err.Error())
       	}


	err = newdb.Close()
	if err != nil {
               	fmt.Println("tsdb close error")
               	fmt.Println(err.Error())
               	return
       	}

	fmt.Println("finish bakup tsdb")
}
