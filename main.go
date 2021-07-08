package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"redis-bigkeys/pkg/worker"
)

func main() {
	log.Println(`开始工作`)

	defer recovery()

	ctx, cancelFn := context.WithCancel(context.Background())

	// 处理退出信号,优雅退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signalChan
		log.Printf(`get signal: %v`, sig)
		cancelFn()
	}()

	go worker.Run(ctx, cancelFn)

	<-ctx.Done()

	time.Sleep(time.Second * 5)

	log.Println(`正常退出`)
}

func recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			log.Println("PanicRecover", fmt.Sprintf("Unhandled error: %v\n stack:%v", err.Error(), string(debug.Stack())))
		} else {
			log.Println("PanicRecover", fmt.Sprintf("Panic: %v\n stack:%v", rec, string(debug.Stack())))
		}
	}
}
