package worker

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/hy0kl/gtools"

	"redis-bigkeys/pkg/config"
	"redis-bigkeys/pkg/wredis"
)

type bigKeysItem struct {
	Database int
	KeyType  string
	KeyName  string
	KeySize  int64
	TTL      int64
	ScanAt   int64
}

type bigKeysList []bigKeysItem

func (s bigKeysList) Len() int {
	return len(s)
}

func (s bigKeysList) Less(i, j int) bool {
	return s[i].KeySize > s[j].KeySize
}

func (s bigKeysList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func write(fp *os.File, box bigKeysList) {
	fp.WriteString("database,type,key,size,ttl,scan_time\n")

	for _, item := range box {
		fp.WriteString(fmt.Sprintf("%d,%s,%s,%dB,%ds,%s\n",
			item.Database, item.KeyType, item.KeyName, item.KeySize, item.TTL, gtools.UnixMsec2Date(item.ScanAt, `Y-m-d H:i:s`)))
	}
}

func isShuttingDown(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true

	default:
		return false
	}
}

func Run(ctx context.Context, cancelFn context.CancelFunc) {
	cfg := config.GetCfg()
	output := cfg.Section(`app`).Key(`output`).String()

	outputFile, err := os.Create(output)
	if err != nil {
		log.Printf(`[Run] can create output file: %s`, output)
		cancelFn()
		return
	}

	defer outputFile.Close()

	var list bigKeysList
	var bytes = cfg.Section(`app`).Key(`bytes`).MustInt64()
	if bytes <= 0 {
		bytes = 1024
	}

	rdsClient := wredis.NewClient()

	var scan uint64

	for {
		if isShuttingDown(ctx) {
			log.Println(`收到退出信号`)
			break
		}

		var scanRet []string
		scanRet, scan = rdsClient.Scan(ctx, scan, ``, 500).Val()

		log.Printf(`scan: %d`, scan)

		for _, key := range scanRet {
			item := bigKeysItem{
				KeyType: rdsClient.Type(ctx, key).Val(),
				KeyName: key,
				TTL:     int64(rdsClient.TTL(ctx, key).Val().Seconds()),
				KeySize: rdsClient.MemoryUsage(ctx, key).Val(),
				ScanAt:  gtools.GetUnixMillis(),
			}

			if item.KeySize >= bytes {
				list = append(list, item)
			}
		}

		if scan == 0 {
			log.Println(`全部扫描完成`)
			break
		}
	}

	write(outputFile, list)

	cancelFn()
}
