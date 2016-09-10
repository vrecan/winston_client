package main

import (
	"fmt"
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	uuid "github.com/satori/go.uuid"
	"github.com/vrecan/death"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"time"
	// "io"
	// "net"
	SYS "syscall"
)

func main() {
	defer log.Flush()
	logger, err := log.LoggerFromConfigAsFile("seelog.xml")

	if err != nil {
		log.Warn("failed to load seelog config", err)
	}

	log.ReplaceLogger(logger)

	death := death.NewDeath(SYS.SIGINT, SYS.SIGTERM)
	grpc.EnableTracing = true
	// flag.Parse()
	// var opts []grpc.DialOption
	// if *tls {
	// 	var sn string
	// 	if *serverHostOverride != "" {
	// 		sn = *serverHostOverride
	// 	}
	// 	var creds credentials.TransportCredentials
	// 	if *caFile != "" {
	// 		var err error
	// 		creds, err = credentials.NewClientTLSFromFile(*caFile, sn)
	// 		if err != nil {
	// 			grpclog.Fatalf("Failed to create TLS credentials %v", err)
	// 		}
	// 	} else {
	// 		creds = credentials.NewClientTLSFromCert(nil, sn)
	// 	}
	// 	opts = append(opts, grpc.WithTransportCredentials(creds))
	// } else {
	// 	opts = append(opts, grpc.WithInsecure())
	// }
	conn, err := grpc.Dial("127.0.0.1:5001", grpc.WithInsecure())
	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewV1Client(conn)
	setupRepo(client)
	go func() {
		for {
			runPush(client)
			// time.Sleep(2 * time.Second)
		}
	}()

	death.WaitForDeath()
	log.Info("shutdown")
}

func setupRepo(client pb.V1Client) {
	_, err := client.UpsertRepo(context.Background(), &pb.RepoSettings{
		Repo:      "name0",
		Format:    pb.RepoSettings_JSON,
		DateField: "normalDate",
		HashField: "id",
		Buckets:   8})
	if err != nil {
		panic(fmt.Sprintf("Failed to setup repo: %v", err))
	}
}

func runPush(client pb.V1Client) {
	// stream, err := client.Push(context.Background())
	// if err != nil {
	// 	grpclog.Fatalf("%v.Push(_) = _, %v", client, err)
	// }
	now := time.Now()
	cnt := 0
	bulkCnt := 2000
	for {

		data := MakeRequests(500, bulkCnt)

		stream, err := client.Push(context.Background())
		if err != nil {
			log.Error("Failed to push stream to context background: ", err)
			return
		}

		for _, note := range data {
			if err := stream.Send(note); err != nil {
				log.Error("stream send: ", err)
				return
			}
			cnt++
		}
		_, err = stream.CloseAndRecv()
		if err != nil {
			log.Error("Transaction failed for stream: ", err)
			return
		}

		if cnt*bulkCnt >= 100000 {
			break
		}
	}

	duration := time.Since(now)
	log.Info(cnt*bulkCnt, " messages in ", duration)

}

func MakeRequests(cnt int, batchCnt int) []*pb.PushRequest {
	data := make([]*pb.PushRequest, 0)

	for i := 0; i < cnt; i++ {
		pr := &pb.PushRequest{
			Repo: "name0",
		}
		for bc := 0; bc < batchCnt; bc++ {
			row := &pb.Row{
				Data: []byte(fmt.Sprintf("{ \"id\": \"%s\" woo this thing is a bunch of extra data that we ddin't have in the thingy magiger bob before. But we do now don't we bob.", uuid.NewV4())),
			}
			pr.Rows = append(pr.Rows, row)
		}
		data = append(data, pr)
	}
	return data
}
