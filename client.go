package main

import (
	"fmt"
	CLI "github.com/LogRhythm/Winston/client"
	pb "github.com/LogRhythm/Winston/pb"
	"github.com/Pallinder/go-randomdata"
	log "github.com/cihub/seelog"
	uuid "github.com/satori/go.uuid"
	"github.com/vrecan/death"
	"golang.org/x/net/context"
	// "google.golang.org/grpc"
	// "google.golang.org/grpc/grpclog"
	"io"
	"time"
	// "net"
	"sync"
	SYS "syscall"
)

const bulkCnt = 100

func main() {
	defer log.Flush()
	logger, err := log.LoggerFromConfigAsFile("seelog.xml")

	if err != nil {
		log.Warn("failed to load seelog config", err)
	}

	log.ReplaceLogger(logger)

	death := death.NewDeath(SYS.SIGINT, SYS.SIGTERM)
	setupRepo()
	// grpc.EnableTracing = true
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
	// conn, err := grpc.Dial("127.0.0.1:5001", grpc.WithInsecure())
	// if err != nil {
	// 	grpclog.Fatalf("fail to dial: %v", err)
	// }
	// defer conn.Close()
	// client := pb.NewV1Client(conn)
	// setupRepo(client)

	go func() {
		for {
			err := runPushCliV2()
			if err != nil {
				log.Error("Failed to push sleeping")
				time.Sleep(2 * time.Second)
			}
		}
	}()

	// go func() {
	// 	for {
	// 		err := runPushCli()
	// 		if err != nil {
	// 			log.Error("Failed to push sleeping")
	// 			time.Sleep(2 * time.Second)
	// 		}
	// 	}
	// }()

	// go func() {
	// 	for {
	// 		QueryByTime()
	// 		time.Sleep(2 * time.Second)

	// 	}
	// }()

	go func() {

		for {

			start := time.Now().Add(-720 * time.Hour)
			end := time.Now()
			repo := "name0"
			partitions, err := getPartitions(repo, start, end)
			if err != nil {
				log.Error("getPartitions: ", err)
				time.Sleep(2 * time.Second)
				continue
			}

			wg := &sync.WaitGroup{}
			for _, path := range partitions {

				wg.Add(1)
				go func(partitionPath string) {
					cli := CLI.NewClient()
					defer wg.Done()
					t := time.Now()
					it := time.Now()
					c, err := cli.QueryPartition(pb.QueryPartition{
						Repo:          repo,
						StartTime:     start.Format(time.RFC3339),
						EndTime:       end.Format(time.RFC3339),
						PartitionPath: partitionPath,
						BatchSize:     50,
					}, 10)
					if err != nil {
						log.Error("QueryPartitions: ", err)
						time.Sleep(2 * time.Second)
						return
					}
					cnt := 0
					rcds := 0
					sampRcds := 0
					for r := range c {
						if r.Err != nil {
							log.Error("query error: ", r.Err)
							continue
						}
						rcds += len(r.R)
						sampRcds += len(r.R)
						if cnt%100 == 0 {
							if len(r.R) > 20 {
								log.Info("0: ", string(r.R[0]))
								log.Info("10: ", string(r.R[10]))
								duration := time.Since(it)
								log.Info("read rate/s: ", float64(sampRcds)/duration.Seconds())
								it = time.Now()
								sampRcds = 0
							}
						}
						cnt++
					}
					duration := time.Since(t)
					log.Info("read msgs: ", cnt, " rcds: ", rcds, " took: ", duration)
				}(path)
			}
			wg.Wait()

		}
	}()

	death.WaitForDeath()
	log.Info("shutdown")
}

func getPartitions(repo string, startDate, endDate time.Time) ([]string, error) {
	cli := CLI.NewClient()
	return cli.GetPartitions(repo, startDate, endDate)
}

func setupRepo() {
	for {
		cli := CLI.NewClient()
		err := cli.UpdateRepository(pb.RepoSettings{
			Repo:              "name0",
			Format:            pb.RepoSettings_JSON,
			TimeField:         "normalDate",
			GroupByFields:     []string{0: "id"},
			GroupByPartitions: 32})
		if err == nil {
			break
		} else {
			log.Error("failed to update settings: ", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func QueryByTime() {
	cli := CLI.NewClient()
	t := time.Now()
	it := time.Now()
	q := pb.Query{
		Repo:      "name0",
		StartTime: t.Add(-720 * time.Hour).Format(time.RFC3339),
		EndTime:   t.Format(time.RFC3339),
		BatchSize: 1000,
	}
	resQ, err := cli.Query(q, 10)
	if err != nil {
		log.Error("query error: ", err)
		return
	}
	cnt := 0
	rcds := 0
	sampRcds := 0
	for r := range resQ {
		if r.Err != nil {
			log.Error("query error: ", r.Err)
			continue
		}
		rcds += len(r.R)
		sampRcds += len(r.R)
		if cnt%100 == 0 {
			if len(r.R) > 20 {
				log.Info("0: ", string(r.R[0]))
				log.Info("10: ", string(r.R[10]))
				duration := time.Since(it)
				log.Info("query all read rate/s: ", float64(sampRcds)/duration.Seconds())
				it = time.Now()
				sampRcds = 0
			}
		}
		cnt++
	}
	duration := time.Since(t)
	log.Info("query all msgs: ", cnt, " rcds: ", rcds, " took: ", duration)
}

func runPullBuckets(client pb.V1Client) {
	t := time.Now()
	it := time.Now()
	pull := pb.Query{
		Repo:      "name0",
		StartTime: t.Add(-720 * time.Hour).Format(time.RFC3339),
		EndTime:   t.Format(time.RFC3339),
		BatchSize: 10,
	}

	stream, err := client.QueryAll(context.Background(), &pull)
	if err != nil {
		log.Error("Failed to get stream from pullBucketByTime: ", err)
		return
	}
	cnt := 0
	rcds := 0
	sampRcds := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			duration := time.Since(t)
			log.Info("msgs: ", cnt, " rcds: ", rcds, " took: ", duration)
			return
		}
		if err != nil {
			log.Error("Failed to read row from winston: ", err, " resp: ", resp)
			return
		}
		rcds += len(resp.Rows)
		sampRcds += len(resp.Rows)
		if cnt%200 == 0 {
			if len(resp.Rows) > 20 {
				log.Info("0: ", resp.Rows[0])
				log.Info("10: ", resp.Rows[10])
				duration := time.Since(it)
				log.Info("read rate/s: ", float64(sampRcds)/duration.Seconds())
				it = time.Now()
				sampRcds = 0
			}
		}
		cnt++
	}
	return

}

func runPushCliV2() error {
	cli := CLI.NewClient()
	defer cli.Close()
	now := time.Now()
	cnt := 5000
	msgs := randomJson(cnt)
	err := cli.Write("name0", msgs)
	if err != nil {
		log.Error("Failed to write: ", err)
		return err
	}
	duration := time.Since(now)
	log.Info("msgs: ", cnt, " in ", duration, " rate: ", float64(cnt)/duration.Seconds())
	return nil
}

func runPushCli() error {
	cli := CLI.NewClient()
	defer cli.Close()
	now := time.Now()
	cnt := 10000
	msgs := makeSlicesOfBytes(cnt)
	err := cli.Write("name0", msgs)
	if err != nil {
		log.Error("Failed to write: ", err)
		return err
	}
	duration := time.Since(now)
	log.Info("msgs: ", cnt, " in ", duration, " rate: ", float64(cnt)/duration.Seconds())
	return nil
}

func runPush(client pb.V1Client) error {
	// stream, err := client.Push(context.Background())
	// if err != nil {
	// 	grpclog.Fatalf("%v.Push(_) = _, %v", client, err)
	// }
	data := MakeRequests(10000, bulkCnt)
	now := time.Now()
	cnt := 0

	for {

		stream, err := client.Write(context.Background())
		if err != nil {
			log.Error("Failed to push stream to context background: ", err)
			return err
		}

		for _, note := range data {
			if err := stream.Send(note); err != nil {
				log.Error("write stream send failed: ", err)
				return err
			}
			cnt++
		}
		_, err = stream.CloseAndRecv()
		if err != nil {
			log.Error("Transaction failed for stream: ", err)
			return err
		}
		break

	}

	duration := time.Since(now)
	log.Info("batches: ", cnt, " msgs: ", cnt*bulkCnt, " in ", duration, " rate: ", float64(cnt*bulkCnt)/duration.Seconds())
	return nil

}

func makeSlicesOfBytes(batchCnt int) [][]byte {
	s := make([][]byte, batchCnt)
	for bc := 0; bc < batchCnt; bc++ {
		s[bc] = []byte(fmt.Sprintf("{ \"id\": \"%s\", \"normalDate\": \"%s\", \"commonEventId\":1021382,\"count\":1,\"direction\":2,\"domain\":\"SECIOUS\",\"entityId\":29,\"impactedEntityId\":29,\"impactedHostId\":175,\"insertedDate\":\"2016-10-07T15:54:46.1812332Z\",\"impactedIp\":\"10.128.64.155\",\"originIp\":\"10.110.0.63\",\"impactedLocationKey\":\"USCO:boulder\",\"logDate\":\"2016-10-07T08:57:19.5871955\",\"login\":\"chris.martin\",\"logSequence\":5973700,\"logSourceId\":924,\"mediatorSessionId\":134158,\"mpeRuleId\":1080954,\"msgClassId\":1060,\"msgSourceTypeId\":1000030,\"impactedName\":\"usbo1pprint01.schq.secious.com\",\"impactedNetworkId\":26,\"normalDate\":\"2016-10-07T14:57:19.7191955Z\",\"object\":\"\\\\\\\\*\\\\IPC$\",\"objectName\":\"spoolss\",\"originPort\":54562,\"priority\":22,\"rootEntityId\":1,\"session\":\"0x6a857adc\",\"severity\":\"Information\",\"vendorMessageId\":\"5145\",\"impactedZoneEnum\":1,\"originZoneEnum\":1,\"impactedGeoPoint\":{\"lat\":39.95960998535156,\"lon\":-105.51024627685547},\"resolvedOriginName\":\"94aeee81-8794-4c31-5ea1-b5b95aec1e6f\",\"resolvedImpactedName\":\"1fae203f-6745-430e-5a4a-61e44e30e288\",\"anonymizedLogin\":\"b7b986f64e2676ee41fc263c8b05a89ccce5a80e645a3a648048fd74e23d012b\",\"indexId\":\"24501ffb-824d-4c45-80c2-5f39ad70a783\",\"#companyName\":\"LogRhythm\"}", uuid.NewV4(), time.Now().Format(time.RFC3339)))
	}
	return s

}

func randomJson(batchCnt int) [][]byte {
	s := make([][]byte, batchCnt)
	for bc := 0; bc < batchCnt; bc++ {
		s[bc] = []byte(fmt.Sprintf("{ \"id\": \"%s\", \"normalDate\": \"%s\", \"first\":\"%s\",\"last\":\"%s\",\"paragrapth\":2,\"domain\":\"SECIOUS\",\"entityId\":29,\"impactedEntityId\":29,\"impactedHostId\":175,\"insertedDate\":\"2016-10-07T15:54:46.1812332Z\",\"impactedIp\":\"10.128.64.155\",\"originIp\":\"10.110.0.63\",\"impactedLocationKey\":\"USCO:boulder\",\"logDate\":\"2016-10-07T08:57:19.5871955\",\"login\":\"chris.martin\",\"logSequence\":5973700,\"logSourceId\":924,\"mediatorSessionId\":134158,\"mpeRuleId\":1080954,\"msgClassId\":1060,\"msgSourceTypeId\":1000030,\"impactedName\":\"usbo1pprint01.schq.secious.com\",\"impactedNetworkId\":26,\"normalDate\":\"2016-10-07T14:57:19.7191955Z\",\"object\":\"\\\\\\\\*\\\\IPC$\",\"objectName\":\"spoolss\",\"originPort\":54562,\"priority\":22,\"rootEntityId\":1,\"session\":\"0x6a857adc\",\"severity\":\"Information\",\"vendorMessageId\":\"5145\",\"impactedZoneEnum\":1,\"originZoneEnum\":1,\"impactedGeoPoint\":{\"lat\":39.95960998535156,\"lon\":-105.51024627685547},\"resolvedOriginName\":\"94aeee81-8794-4c31-5ea1-b5b95aec1e6f\",\"resolvedImpactedName\":\"1fae203f-6745-430e-5a4a-61e44e30e288\",\"anonymizedLogin\":\"b7b986f64e2676ee41fc263c8b05a89ccce5a80e645a3a648048fd74e23d012b\",\"paragraph\":\"%s\",\"#companyName\":\"LogRhythm\"}", uuid.NewV4(), time.Now().Format(time.RFC3339), randomdata.FirstName(randomdata.RandomGender), randomdata.LastName(), randomdata.Paragraph()))
	}
	return s

}

func MakeRequests(cnt int, batchCnt int) []*pb.WriteRequest {
	data := make([]*pb.WriteRequest, 0)

	for i := 0; i < cnt; i++ {
		pr := &pb.WriteRequest{
			Repo: "name0",
		}
		for bc := 0; bc < batchCnt; bc++ {
			row := &pb.Row{
				Data: []byte(fmt.Sprintf("{ \"id\": \"%s\", \"normalDate\": \"%s\", \"commonEventId\":1021382,\"count\":1,\"direction\":2,\"domain\":\"SECIOUS\",\"entityId\":29,\"impactedEntityId\":29,\"impactedHostId\":175,\"insertedDate\":\"2016-10-07T15:54:46.1812332Z\",\"impactedIp\":\"10.128.64.155\",\"originIp\":\"10.110.0.63\",\"impactedLocationKey\":\"USCO:boulder\",\"logDate\":\"2016-10-07T08:57:19.5871955\",\"login\":\"chris.martin\",\"logSequence\":5973700,\"logSourceId\":924,\"mediatorSessionId\":134158,\"mpeRuleId\":1080954,\"msgClassId\":1060,\"msgSourceTypeId\":1000030,\"impactedName\":\"usbo1pprint01.schq.secious.com\",\"impactedNetworkId\":26,\"normalDate\":\"2016-10-07T14:57:19.7191955Z\",\"object\":\"\\\\\\\\*\\\\IPC$\",\"objectName\":\"spoolss\",\"originPort\":54562,\"priority\":22,\"rootEntityId\":1,\"session\":\"0x6a857adc\",\"severity\":\"Information\",\"vendorMessageId\":\"5145\",\"impactedZoneEnum\":1,\"originZoneEnum\":1,\"impactedGeoPoint\":{\"lat\":39.95960998535156,\"lon\":-105.51024627685547},\"resolvedOriginName\":\"94aeee81-8794-4c31-5ea1-b5b95aec1e6f\",\"resolvedImpactedName\":\"1fae203f-6745-430e-5a4a-61e44e30e288\",\"anonymizedLogin\":\"b7b986f64e2676ee41fc263c8b05a89ccce5a80e645a3a648048fd74e23d012b\",\"indexId\":\"24501ffb-824d-4c45-80c2-5f39ad70a783\",\"#companyName\":\"LogRhythm\"}", uuid.NewV4(), time.Now().Format(time.RFC3339))),
			}
			pr.Rows = append(pr.Rows, row)
		}
		data = append(data, pr)
	}
	return data
}
