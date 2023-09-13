module github.com/withqb/coddy

go 1.21

replace (

github.com/withqb/xcore => ../pkg/xcore
github.com/withqb/xlogrus => ../pkg/xlogrus
github.com/withqb/xtools => ../pkg/xtools
github.com/withqb/xutil => ../pkg/xutil

)

require (
	github.com/Arceliar/phony v0.0.0-20220903101357-530938a4b13d
	github.com/MFAshby/stdemuxerhook v1.0.0
	github.com/blevesearch/bleve/v2 v2.3.9
	github.com/dgraph-io/ristretto v0.1.1
	github.com/getsentry/sentry-go v0.24.1
	github.com/google/uuid v1.3.1
	github.com/gorilla/mux v1.8.0
	github.com/kardianos/minwinsvc v1.0.2
	github.com/lib/pq v1.10.9
	github.com/nats-io/nats-server/v2 v2.9.22
	github.com/nats-io/nats.go v1.29.0
	github.com/nfnt/resize v0.0.0-20180221191011-83c6a9932646
	github.com/opentracing/opentracing-go v1.2.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.16.0
	github.com/sirupsen/logrus v1.9.3
	github.com/tidwall/gjson v1.16.0
	github.com/tidwall/sjson v1.2.5
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	github.com/withqb/xcore v0.0.1
	github.com/withqb/xlogrus v0.0.1
	github.com/withqb/xtools v0.0.2
	github.com/withqb/xutil v0.0.1
	go.uber.org/atomic v1.11.0
	golang.org/x/crypto v0.13.0
	golang.org/x/exp v0.0.0-20230905200255-921286631fa9
	golang.org/x/image v0.12.0
	golang.org/x/sync v0.3.0
	golang.org/x/term v0.12.0
	gopkg.in/h2non/bimg.v1 v1.1.9
	gopkg.in/yaml.v2 v2.4.0
	modernc.org/sqlite v1.25.0
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/RoaringBitmap/roaring v1.2.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/blevesearch/bleve_index_api v1.0.5 // indirect
	github.com/blevesearch/geo v0.1.17 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.1.5 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.2 // indirect
	github.com/blevesearch/vellum v1.0.10 // indirect
	github.com/blevesearch/zapx/v11 v11.3.9 // indirect
	github.com/blevesearch/zapx/v12 v12.3.9 // indirect
	github.com/blevesearch/zapx/v13 v13.3.9 // indirect
	github.com/blevesearch/zapx/v14 v14.3.9 // indirect
	github.com/blevesearch/zapx/v15 v15.3.12 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/mattn/go-sqlite3 v1.14.17 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/nats-io/jwt/v2 v2.5.0 // indirect
	github.com/nats-io/nkeys v0.4.4 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.10.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	go.etcd.io/bbolt v1.3.7 // indirect
	golang.org/x/mod v0.12.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.13.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/macaroon.v2 v2.1.0 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.40.0 // indirect
	modernc.org/ccgo/v3 v3.16.13 // indirect
	modernc.org/libc v1.24.1 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.6.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.0.1 // indirect
)
