/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package juicefs

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/juicedata/juicefs/pkg/fs" //nolint:gofumpt
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time" //nolint:gofumpt

	"github.com/erikdubbelboer/gspt"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/metric"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/usage"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/minio/cli"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/pyroscope-io/client/pyroscope"
	"github.com/sirupsen/logrus"
)

var debugAgent string
var debugAgentOnce sync.Once

func getMetaConf(c *cli.Context, mp string, readOnly bool) *meta.Config {
	conf := meta.DefaultConf()
	conf.Retries = c.Int("io-retries")
	conf.MaxDeletes = c.Int("max-deletes")
	conf.SkipDirNlink = c.Int("skip-dir-nlink")
	conf.ReadOnly = readOnly
	conf.NoBGJob = c.Bool("no-bgjob")
	conf.OpenCache = time.Duration(c.Float64("open-cache") * 1e9)
	conf.OpenCacheLimit = c.Uint64("open-cache-limit")
	conf.Heartbeat = duration(c.String("heartbeat"))
	conf.MountPoint = mp
	conf.Subdir = c.String("subdir")

	atimeMode := c.String("atime-mode")
	if atimeMode != meta.RelAtime && atimeMode != meta.StrictAtime && atimeMode != meta.NoAtime {
		logger.Warnf("unknown atime-mode \"%s\", changed to %s", atimeMode, meta.NoAtime)
		atimeMode = meta.NoAtime
	}
	conf.AtimeMode = atimeMode
	return conf
}

func duration(s string) time.Duration {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Second * time.Duration(v)
	}
	if v, err := time.ParseDuration(s); err == nil {
		return v
	}
	return 0
}

func wrapRegister(mp, name string) (prometheus.Registerer, *prometheus.Registry) {
	registry := prometheus.NewRegistry() // replace default so only JuiceFS metrics are exposed
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": name}, registry))
	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())
	return registerer, registry
}

func exposeMetrics(c *cli.Context, m meta.Meta, registerer prometheus.Registerer, registry *prometheus.Registry) string {
	var ip, port string
	//default set
	ip, port, err := net.SplitHostPort(c.String("metrics"))
	if err != nil {
		logger.Fatalf("metrics format error: %v", err)
	}

	m.InitMetrics(registerer)
	vfs.InitMetrics(registerer)
	go metric.UpdateMetrics(m, registerer)
	http.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	registerer.MustRegister(collectors.NewBuildInfoCollector())

	// If not set metrics addr,the port will be auto set
	if !c.IsSet("metrics") {
		// If only set consul, ip will auto set
		if c.IsSet("consul") {
			ip, err = utils.GetLocalIp(c.String("consul"))
			if err != nil {
				logger.Errorf("Get local ip failed: %v", err)
				return ""
			}
		}
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(ip, port))
	if err != nil {
		// Don't try other ports on metrics set but listen failed
		if c.IsSet("metrics") {
			logger.Errorf("listen on %s:%s failed: %v", ip, port, err)
			return ""
		}
		// Listen port on 0 will auto listen on a free port
		ln, err = net.Listen("tcp", net.JoinHostPort(ip, "0"))
		if err != nil {
			logger.Errorf("Listen failed: %v", err)
			return ""
		}
	}

	go func() {
		if err := http.Serve(ln, nil); err != nil {
			logger.Errorf("Serve for metrics: %s", err)
		}
	}()

	metricsAddr := ln.Addr().String()
	logger.Infof("Prometheus metrics listening on %s", metricsAddr)
	return metricsAddr
}

func initBackgroundTasks(c *cli.Context, vfsConf *vfs.Config, metaConf *meta.Config, m meta.Meta, blob object.ObjectStorage, registerer prometheus.Registerer, registry *prometheus.Registry) {
	metricsAddr := exposeMetrics(c, m, registerer, registry)
	vfsConf.Port.PrometheusAgent = metricsAddr
	if c.IsSet("consul") {
		metric.RegisterToConsul(c.String("consul"), metricsAddr, vfsConf.Meta.MountPoint)
		vfsConf.Port.ConsulAddr = c.String("consul")
	}
	if !metaConf.ReadOnly && !metaConf.NoBGJob && vfsConf.BackupMeta > 0 {
		go vfs.Backup(m, blob, vfsConf.BackupMeta)
	}
	if !c.Bool("no-usage-report") {
		go usage.ReportUsage(m, version.Version())
	}
}

func getChunkConf(c *cli.Context, format *meta.Format) *chunk.Config {
	cm, err := strconv.ParseUint(c.String("cache-mode"), 8, 32)
	if err != nil {
		logger.Warnf("Invalid cache-mode %s, using default value 0600", c.String("cache-mode"))
		cm = 0600
	}
	chunkConf := &chunk.Config{
		BlockSize:  format.BlockSize * 1024,
		Compress:   format.Compression,
		HashPrefix: format.HashPrefix,

		GetTimeout:    time.Second * time.Duration(c.Int("get-timeout")),
		PutTimeout:    time.Second * time.Duration(c.Int("put-timeout")),
		MaxUpload:     c.Int("max-uploads"),
		MaxRetries:    c.Int("io-retries"),
		Writeback:     c.Bool("writeback"),
		Prefetch:      c.Int("prefetch"),
		BufferSize:    c.Int("buffer-size") << 20,
		UploadLimit:   c.Int64("upload-limit") * 1e6 / 8,
		DownloadLimit: c.Int64("download-limit") * 1e6 / 8,
		UploadDelay:   duration(c.String("upload-delay")),

		CacheDir:          c.String("cache-dir"),
		CacheSize:         int64(c.Int("cache-size")),
		FreeSpace:         float32(c.Float64("free-space-ratio")),
		CacheMode:         os.FileMode(cm),
		CacheFullBlock:    !c.Bool("cache-partial-only"),
		CacheChecksum:     c.String("verify-cache-checksum"),
		CacheEviction:     c.String("cache-eviction"),
		CacheScanInterval: duration(c.String("cache-scan-interval")),
		AutoCreate:        true,
	}
	if chunkConf.UploadLimit == 0 {
		chunkConf.UploadLimit = format.UploadLimit * 1e6 / 8
	}
	if chunkConf.DownloadLimit == 0 {
		chunkConf.DownloadLimit = format.DownloadLimit * 1e6 / 8
	}
	chunkConf.SelfCheck(format.UUID)
	return chunkConf
}

type storageHolder struct {
	object.ObjectStorage
	fmt meta.Format
}

func createStorage(format meta.Format) (object.ObjectStorage, error) {
	if err := format.Decrypt(); err != nil {
		return nil, fmt.Errorf("format decrypt: %s", err)
	}
	object.UserAgent = "JuiceFS-" + version.Version()
	var blob object.ObjectStorage
	var err error
	if u, err := url.Parse(format.Bucket); err == nil {
		values := u.Query()
		if values.Get("tls-insecure-skip-verify") != "" {
			var tlsSkipVerify bool
			if tlsSkipVerify, err = strconv.ParseBool(values.Get("tls-insecure-skip-verify")); err != nil {
				return nil, err
			}
			object.GetHttpClient().Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify = tlsSkipVerify
			values.Del("tls-insecure-skip-verify")
			u.RawQuery = values.Encode()
			format.Bucket = u.String()
		}
	}

	if format.Shards > 1 {
		blob, err = object.NewSharded(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken, format.Shards)
	} else {
		blob, err = object.CreateStorage(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken)
	}
	if err != nil {
		return nil, err
	}
	blob = object.WithPrefix(blob, format.Name+"/")
	if format.StorageClass != "" {
		if os, ok := blob.(object.SupportStorageClass); ok {
			os.SetStorageClass(format.StorageClass)
		}
	}
	if format.EncryptKey != "" {
		passphrase := os.Getenv("JFS_RSA_PASSPHRASE")
		if passphrase == "" {
			block, _ := pem.Decode([]byte(format.EncryptKey))
			// nolint:staticcheck
			if block != nil && strings.Contains(block.Headers["Proc-Type"], "ENCRYPTED") && x509.IsEncryptedPEMBlock(block) {
				return nil, fmt.Errorf("passphrase is required to private key, please try again after setting the 'JFS_RSA_PASSPHRASE' environment variable")
			}
		}

		privKey, err := object.ParseRsaPrivateKeyFromPem([]byte(format.EncryptKey), []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("parse rsa: %s", err)
		}
		encryptor, err := object.NewDataEncryptor(object.NewRSAEncryptor(privKey), format.EncryptAlgo)
		if err != nil {
			return nil, err
		}
		blob = object.NewEncrypted(blob, encryptor)
	}
	return blob, nil
}

func NewReloadableStorage(format *meta.Format, cli meta.Meta, patch func(*meta.Format)) (object.ObjectStorage, error) {
	if patch != nil {
		patch(format)
	}
	blob, err := createStorage(*format)
	if err != nil {
		return nil, err
	}
	holder := &storageHolder{
		ObjectStorage: blob,
		fmt:           *format, // keep a copy to find the change
	}
	cli.OnReload(func(new *meta.Format) {
		if patch != nil {
			patch(new)
		}
		old := &holder.fmt
		if new.Storage != old.Storage || new.Bucket != old.Bucket || new.AccessKey != old.AccessKey || new.SecretKey != old.SecretKey || new.SessionToken != old.SessionToken || new.StorageClass != old.StorageClass {
			logger.Infof("found new configuration: storage=%s bucket=%s ak=%s storageClass=%s", new.Storage, new.Bucket, new.AccessKey, new.StorageClass)

			newBlob, err := createStorage(*new)
			if err != nil {
				logger.Warnf("object storage: %s", err)
				return
			}
			holder.ObjectStorage = newBlob
			holder.fmt = *new
		}
	})
	return holder, nil
}

func updateFormat(c *cli.Context) func(*meta.Format) {
	return func(format *meta.Format) {
		if c.IsSet("bucket") {
			format.Bucket = c.String("bucket")
		}
		if c.IsSet("storage") {
			format.Storage = c.String("storage")
		}
		if c.IsSet("storage-class") {
			format.StorageClass = c.String("storage-class")
		}
		if c.IsSet("upload-limit") {
			format.UploadLimit = c.Int64("upload-limit")
		}
		if c.IsSet("download-limit") {
			format.DownloadLimit = c.Int64("download-limit")
		}
	}
}

func initForSvc(c *cli.Context, mp string, metaUrl string) (*vfs.Config, *fs.FileSystem) {
	removePassword(metaUrl)
	metaConf := getMetaConf(c, mp, c.Bool("read-only"))
	metaCli := meta.NewClient(metaUrl, metaConf)
	format, err := metaCli.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}
	if st := metaCli.Chroot(meta.Background, metaConf.Subdir); st != 0 {
		logger.Fatalf("Chroot to %s: %s", metaConf.Subdir, st)
	}
	registerer, registry := wrapRegister(mp, format.Name)

	blob, err := NewReloadableStorage(format, metaCli, updateFormat(c))
	if err != nil {
		logger.Fatalf("object storage: %s", err)
	}
	logger.Infof("Data use %s", blob)

	chunkConf := getChunkConf(c, format)
	store := chunk.NewCachedStore(blob, *chunkConf, registerer)
	registerMetaMsg(metaCli, store, chunkConf)

	err = metaCli.NewSession(true)
	if err != nil {
		logger.Fatalf("new session: %s", err)
	}
	metaCli.OnReload(func(fmt *meta.Format) {
		updateFormat(c)(fmt)
		store.UpdateLimit(fmt.UploadLimit, fmt.DownloadLimit)
	})

	// Go will catch all the signals
	signal.Ignore(syscall.SIGPIPE)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		sig := <-signalChan
		logger.Infof("Received signal %s, exiting...", sig.String())
		if err := metaCli.CloseSession(); err != nil {
			logger.Fatalf("close session failed: %s", err)
		}
		os.Exit(0)
	}()
	vfsConf := getVfsConf(c, metaConf, format, chunkConf)
	vfsConf.AccessLog = c.String("access-log")
	vfsConf.AttrTimeout = time.Millisecond * time.Duration(c.Float64("attr-cache")*1000)
	vfsConf.EntryTimeout = time.Millisecond * time.Duration(c.Float64("entry-cache")*1000)
	vfsConf.DirEntryTimeout = time.Millisecond * time.Duration(c.Float64("dir-entry-cache")*1000)

	initBackgroundTasks(c, vfsConf, metaConf, metaCli, blob, registerer, registry)
	jfs, err := fs.NewFileSystem(vfsConf, metaCli, store)
	if err != nil {
		logger.Fatalf("Initialize failed: %s", err)
	}
	jfs.InitMetrics(registerer)

	return vfsConf, jfs
}

func getVfsConf(c *cli.Context, metaConf *meta.Config, format *meta.Format, chunkConf *chunk.Config) *vfs.Config {
	cfg := &vfs.Config{
		Meta:           metaConf,
		Format:         *format,
		Version:        version.Version(),
		Chunk:          chunkConf,
		BackupMeta:     duration(c.String("backup-meta")),
		Port:           &vfs.Port{DebugAgent: debugAgent, PyroscopeAddr: c.String("pyroscope")},
		PrefixInternal: c.Bool("prefix-internal"),
	}
	skip_check := os.Getenv("SKIP_BACKUP_META_CHECK") == "true"
	if !skip_check && cfg.BackupMeta > 0 && cfg.BackupMeta < time.Minute*5 {
		logger.Fatalf("backup-meta should not be less than 5 minutes: %s", cfg.BackupMeta)
	}
	return cfg
}

func registerMetaMsg(m meta.Meta, store chunk.ChunkStore, chunkConf *chunk.Config) {
	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return vfs.Compact(*chunkConf, store, args[0].([]meta.Slice), args[1].(uint64))
	})
}

func removePassword(uri string) {
	args := make([]string, len(os.Args))
	copy(args, os.Args)
	uri2 := utils.RemovePassword(uri)
	if uri2 != uri {
		for i, a := range os.Args {
			if a == uri {
				args[i] = uri2
				break
			}
		}
	}
	gspt.SetProcTitle(strings.Join(args, " "))
}

func setup(c *cli.Context, n int) {
	if c.NArg() < n {
		fmt.Printf("ERROR: This command requires at least %d arguments\n", n)
		fmt.Printf("USAGE:\n   juicefs %s [command options] %s\n", c.Command.Name, c.Command.ArgsUsage)
		os.Exit(1)
	}

	if c.Bool("trace") {
		utils.SetLogLevel(logrus.TraceLevel)
	} else if c.Bool("verbose") {
		utils.SetLogLevel(logrus.DebugLevel)
	} else if c.Bool("quiet") {
		utils.SetLogLevel(logrus.WarnLevel)
	} else {
		utils.SetLogLevel(logrus.InfoLevel)
	}
	if c.Bool("no-color") {
		utils.DisableLogColor()
	}

	if !c.Bool("no-agent") {
		go debugAgentOnce.Do(func() {
			for port := 6060; port < 6100; port++ {
				debugAgent = fmt.Sprintf("127.0.0.1:%d", port)
				_ = http.ListenAndServe(debugAgent, nil)
			}
		})
	}

	if c.IsSet("pyroscope") {
		tags := make(map[string]string)
		appName := fmt.Sprintf("juicefs.%s", c.Command.Name)
		if c.Command.Name == "mount" {
			tags["mountpoint"] = c.Args().Get(1)
		}
		if hostname, err := os.Hostname(); err == nil {
			tags["hostname"] = hostname
		}
		tags["pid"] = strconv.Itoa(os.Getpid())
		tags["version"] = version.Version()

		if _, err := pyroscope.Start(pyroscope.Config{
			ApplicationName: appName,
			ServerAddress:   c.String("pyroscope"),
			Logger:          logger,
			Tags:            tags,
			AuthToken:       os.Getenv("PYROSCOPE_AUTH_TOKEN"),
			ProfileTypes:    pyroscope.DefaultProfileTypes,
		}); err != nil {
			logger.Errorf("start pyroscope agent: %v", err)
		}
	}
}

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:  "verbose",
			Usage: "enable debug log",
		},
		// -- quiet used by minio
		&cli.BoolFlag{
			Name:  "trace",
			Usage: "enable trace log",
		},
		&cli.StringFlag{
			Name:   "log-level",
			Usage:  "set log level (trace, debug, info, warn, error, fatal, panic)",
			Hidden: true,
		},
		&cli.BoolFlag{
			Name:  "no-agent",
			Usage: "disable pprof (:6060) agent",
		},
		&cli.StringFlag{
			Name:  "pyroscope",
			Usage: "pyroscope address",
		},
		&cli.BoolFlag{
			Name:  "no-color",
			Usage: "disable colors",
		},
	}
}

func clientFlags(defaultEntryCache float64) []cli.Flag {
	return expandFlags(
		metaFlags(),
		metaCacheFlags(defaultEntryCache),
		storageFlags(),
		dataCacheFlags(),
	)
}

func metaFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "subdir",
			Usage: "mount a sub-directory as root",
		},
		&cli.StringFlag{
			Name:  "backup-meta",
			Value: "3600",
			Usage: "interval (in seconds) to automatically backup metadata in the object storage (0 means disable backup)",
		},
		&cli.StringFlag{
			Name:  "heartbeat",
			Value: "12",
			Usage: "interval (in seconds) to send heartbeat; it's recommended that all clients use the same heartbeat value",
		},
		&cli.BoolFlag{
			Name:  "read-only",
			Usage: "allow lookup/read operations only",
		},
		&cli.BoolFlag{
			Name:  "no-bgjob",
			Usage: "disable background jobs (clean-up, backup, etc.)",
		},
		&cli.StringFlag{
			Name:  "atime-mode",
			Value: "noatime",
			Usage: "when to update atime, supported mode includes: noatime, relatime, strictatime",
		},
		&cli.IntFlag{
			Name:  "skip-dir-nlink",
			Value: 20,
			Usage: "number of retries after which the update of directory nlink will be skipped (used for tkv only, 0 means never)",
		},
	}
}

func dataCacheFlags() []cli.Flag {
	var defaultCacheDir = "/var/jfsCache"
	switch runtime.GOOS {
	case "linux":
		if os.Getuid() == 0 {
			break
		}
		fallthrough
	case "darwin":
		fallthrough
	case "windows":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
			return nil
		}
		defaultCacheDir = path.Join(homeDir, ".juicefs", "cache")
	}
	return []cli.Flag{
		&cli.IntFlag{
			Name:  "buffer-size",
			Value: 300,
			Usage: "total read/write buffering in MB",
		},
		&cli.IntFlag{
			Name:  "prefetch",
			Value: 1,
			Usage: "prefetch N blocks in parallel",
		},
		&cli.BoolFlag{
			Name:  "writeback",
			Usage: "upload objects in background",
		},
		&cli.StringFlag{
			Name:  "upload-delay",
			Value: "0",
			Usage: "delayed duration (in seconds) for uploading objects",
		},
		&cli.StringFlag{
			Name:  "cache-dir",
			Value: defaultCacheDir,
			Usage: "directory paths of local cache, use colon to separate multiple paths",
		},
		&cli.StringFlag{
			Name:  "cache-mode",
			Value: "0600", // only owner can read/write cache
			Usage: "file permissions for cached blocks",
		},
		&cli.IntFlag{
			Name:  "cache-size",
			Value: 100 << 10,
			Usage: "size of cached object for read in MiB",
		},
		&cli.Float64Flag{
			Name:  "free-space-ratio",
			Value: 0.1,
			Usage: "min free space (ratio)",
		},
		&cli.BoolFlag{
			Name:  "cache-partial-only",
			Usage: "cache only random/small read",
		},
		&cli.StringFlag{
			Name:  "verify-cache-checksum",
			Value: "full",
			Usage: "checksum level (none, full, shrink, extend)",
		},
		&cli.StringFlag{
			Name:  "cache-eviction",
			Value: "2-random",
			Usage: "cache eviction policy (none or 2-random)",
		},
		&cli.StringFlag{
			Name:  "cache-scan-interval",
			Value: "3600",
			Usage: "interval (in seconds) to scan cache-dir to rebuild in-memory index",
		},
	}
}

func storageFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "storage",
			Usage: "customized storage type (e.g. s3, gcs, oss, cos) to access object store",
		},
		&cli.StringFlag{
			Name:  "bucket",
			Usage: "customized endpoint to access object store",
		},
		&cli.StringFlag{
			Name:  "storage-class",
			Usage: "the storage class for data written by current client",
		},
		&cli.IntFlag{
			Name:  "get-timeout",
			Value: 60,
			Usage: "the max number of seconds to download an object",
		},
		&cli.IntFlag{
			Name:  "put-timeout",
			Value: 60,
			Usage: "the max number of seconds to upload an object",
		},
		&cli.IntFlag{
			Name:  "io-retries",
			Value: 10,
			Usage: "number of retries after network failure",
		},
		&cli.IntFlag{
			Name:  "max-uploads",
			Value: 20,
			Usage: "number of connections to upload",
		},
		&cli.IntFlag{
			Name:  "max-deletes",
			Value: 10,
			Usage: "number of threads to delete objects",
		},
		&cli.Int64Flag{
			Name:  "upload-limit",
			Usage: "bandwidth limit for upload in Mbps",
		},
		&cli.Int64Flag{
			Name:  "download-limit",
			Usage: "bandwidth limit for download in Mbps",
		},
	}
}

func metaCacheFlags(defaultEntryCache float64) []cli.Flag {
	return []cli.Flag{
		&cli.Float64Flag{
			Name:  "attr-cache",
			Value: 1.0,
			Usage: "attributes cache timeout in seconds",
		},
		&cli.Float64Flag{
			Name:  "entry-cache",
			Value: defaultEntryCache,
			Usage: "file entry cache timeout in seconds",
		},
		&cli.Float64Flag{
			Name:  "dir-entry-cache",
			Value: 1.0,
			Usage: "dir entry cache timeout in seconds",
		},
		&cli.Float64Flag{
			Name:  "open-cache",
			Value: 0.0,
			Usage: "The seconds to reuse open file without checking update (0 means disable this feature)",
		},
		&cli.IntFlag{
			Name:  "open-cache-limit",
			Value: 10000,
			Usage: "max number of open files to cache (soft limit, 0 means unlimited)",
		},
	}
}

func shareInfoFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "metrics",
			Value: "127.0.0.1:9567",
			Usage: "address to export metrics",
		},
		&cli.StringFlag{
			Name:  "consul",
			Value: "127.0.0.1:8500",
			Usage: "consul address to register",
		},
		&cli.BoolFlag{
			Name:  "no-usage-report",
			Usage: "do not send usage report",
		},
	}
}

func expandFlags(compoundFlags ...[]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, flag := range compoundFlags {
		flags = append(flags, flag...)
	}
	return flags
}
