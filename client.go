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
	"io"
	"time"
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
			// runPush(client)
			runPullBuckets(client)
			
			time.Sleep(2 * time.Second)
		}
	}()
	// go func() {
	// 	for {
	// 		runPush(client)
	// 		time.Sleep(2 * time.Second)
	// 	}
	// }()

	// go func() {
	// 	for {
	// 		runPullBuckets(client)
	// 		time.Sleep(2 * time.Second)
	// 	}
	// }()
	// go func() {
	// 	for {
	// 		runPullBuckets(client)
	// 		time.Sleep(2 * time.Second)
	// 	}
	// }()

	death.WaitForDeath()
	log.Info("shutdown")
}

func getBuckets(client pb.V1Client) []string {
	stream, err := client.GetBuckets(context.Background(), &pb.BucketsRequest{
		Repo:        "name0",
		StartTimeMs: uint64(time.Now().Add(-72*time.Hour).UnixNano() / int64(time.Millisecond)),
		EndTimeMs:   uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	})
	if err != nil {
		log.Error("get buckets: ", err)
	}
	buckets := []string{}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			log.Info("Finished getting buckets")
			return buckets
		}
		log.Info("BUCKET RECV: ", in)
		buckets = append(buckets, in.Path)

	}
	return buckets
}

func setupRepo(client pb.V1Client) {
	_, err := client.UpsertRepo(context.Background(), &pb.RepoSettings{
		Repo:           "name0",
		Format:         pb.RepoSettings_JSON,
		TimeField:      "normalDate",
		GroupByFields:  []string{0: "id"},
		GroupByBuckets: 8})
	if err != nil {
		panic(fmt.Sprintf("Failed to setup repo: %v", err))
	}
}

func runPullBuckets(client pb.V1Client) {
	buckets := getBuckets(client)
	
	for _, b := range buckets {
		t := time.Now()
		func() {
			pull := pb.ReadBucket{
				Repo:        "name0",
				StartTimeMs: uint64(t.Add(-24*time.Hour).UnixNano() / int64(time.Millisecond)),
				EndTimeMs:   uint64(t.UnixNano() / int64(time.Millisecond)),
				BucketPath:  b,
			}
			stream, err := client.ReadBucketByTime(context.Background(), &pull)
			if err != nil {
				log.Error("Failed to get stream from pullBucketByTime: ", err)
				return
			}
			cnt := 0
			rcds := 0
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					duration := time.Since(t)
					log.Info("msgs rcvd: ", cnt, " ", duration, " bucket: ", b)
					log.Info("rcds rcvd: ", rcds, " ", duration, " bucket: ", b)
					return
				}
				if err != nil {
					log.Error("Failed to read row from winston: ", err, " resp: ", resp)
					return
				}
				rcds += len(resp.Rows)
				cnt++
			}
		}()
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

		data := MakeRequests(50, bulkCnt)

		stream, err := client.Write(context.Background())
		if err != nil {
			log.Error("Failed to push stream to context background: ", err)
			return
		}

		for _, note := range data {
			if err := stream.Send(note); err != nil {
				log.Error("write stream send: ", err)
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

func MakeRequests(cnt int, batchCnt int) []*pb.WriteRequest {
	data := make([]*pb.WriteRequest, 0)

	for i := 0; i < cnt; i++ {
		pr := &pb.WriteRequest{
			Repo: "name0",
		}
		for bc := 0; bc < batchCnt; bc++ {
			row := &pb.Row{
				Data: []byte(fmt.Sprintf("{ \"id\": \"%s\", \"normalDate\": \"%s\",  \"woo\" : \"this thing\", \"is\": \"a bunch of extra data that we ddin't have in the thingy magiger bob before. But we do now don't we bob.\"}", uuid.NewV4(), time.Now().Format(time.RFC3339))),
			}
			pr.Rows = append(pr.Rows, row)
		}
		data = append(data, pr)
	}
	return data
}
