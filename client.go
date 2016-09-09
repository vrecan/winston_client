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

	go runPush(client)

	death.WaitForDeath()
	log.Info("shutdown")
}
func runPush(client pb.V1Client) {
	// stream, err := client.Push(context.Background())
	// if err != nil {
	// 	grpclog.Fatalf("%v.Push(_) = _, %v", client, err)
	// }
	now := time.Now()
	cnt := 0
	for {

		data := MakeRequests(100000)

		stream, err := client.Push(context.Background())
		if err != nil {
			log.Error("Failed to push stream to context background: ", err)
		}

		for _, note := range data {
			if err := stream.Send(note); err != nil {
				log.Critical("Failed to send a data: %v", err)
			}
			cnt++
		}
		stream.CloseAndRecv()

		if cnt >= 100000 {
			break
		}
	}

	duration := time.Since(now)
	log.Info(cnt, " messages in ", duration)

}

func MakeRequests(cnt int) []*pb.PushRequest {
	data := make([]*pb.PushRequest, 0)
	for i := 0; i < cnt; i++ {
		data = append(data, &pb.PushRequest{Repo: "name0", Data: []byte(fmt.Sprintf("{ \"id\": \"%s\" woo this thing is a bunch of extra data that we ddin't have in the thingy magiger bob before. But we do now don't we bob.", uuid.NewV4())), Buckets: 0})
	}
	return data
}
