package remoteread

import (
   "net/http"
   "flag"
   "fmt"
   "time"
   "os"
   "strconv"

   "github.com/go-kit/kit/log"
   "github.com/prometheus/prometheus/storage/remote"
   "github.com/prometheus/prometheus/prompb"
   "github.com/prometheus/tsdb"
   "github.com/prometheus/client_golang/prometheus"
   "github.com/prometheus/prometheus/pkg/labels"
)

type myHandler struct{
    db *tsdb.DB
}

func (this myHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    	req, err := remote.DecodeReadRequest(r)
        if err != nil {
                http.Error(w, err.Error(), http.StatusBadRequest)
                return
        }

        resp := prompb.ReadResponse{
                Results: make([]*prompb.QueryResult, len(req.Queries)),
        }

        for i, query := range req.Queries {
                from, through, matchers, selectParams, err := remote.FromQuery(query)
                if err != nil {
                        http.Error(w, err.Error(), http.StatusBadRequest)
                        return
                }

                querier, err := this.db.Querier(from, through)
                if err != nil {
                        http.Error(w, err.Error(), http.StatusInternalServerError)
                        return
                }

		filteredMatchers := make([]*labels.Matcher, 0, len(matchers))
                for _, m := range matchers {
                    filteredMatchers = append(filteredMatchers, m)
		}

                set, err := querier.Select(filteredMatchers...)
                if err != nil {
                        http.Error(w, err.Error(), http.StatusInternalServerError)
                        return
                }
                resp.Results[i], err = remote.ToQueryResult(set, 5e7)
                if err != nil {
                        if httpErr, ok := err.(remote.HTTPError); ok {
                                http.Error(w, httpErr.Error(), httpErr.Status())
                                return
                        }
                        http.Error(w, err.Error(), http.StatusInternalServerError)
                        return
                }
        }

        if err := remote.EncodeReadResponse(&resp, w); err != nil {
                http.Error(w, err.Error(), http.StatusInternalServerError)
                return
        }
}

func main() {

    var logger log.Logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

    dbpath := flag.String("dbpath", "/root/lm/bkup/data", "a string")
    port := flag.Int("port", 8080, "a int")

    flag.Parse()

    var reg = prometheus.NewRegistry()

    db, err := tsdb.Open(*dbpath, log.With(logger, "db", "tsdb"), reg, tsdb.DefaultOptions)

    if err != nil {
        fmt.Println("Open tsdb error")
        fmt.Println(err.Error())
        return
    }

    addr := ":" + strconv.Itoa(*port)

    server := http.Server{
        Addr:	addr,
        Handler:	&myHandler{db: db},
        ReadTimeout:	300*time.Second,
    }
}
