package main

import (
	"fmt"
	"github.com/Pallinder/go-randomdata"
	log "github.com/cihub/seelog"
	uuid "github.com/satori/go.uuid"
	"github.com/vrecan/death"
	CLI "github.schq.secious.com/UltraViolet/Winston/client"
	pb "github.schq.secious.com/UltraViolet/Winston/pb"
	"golang.org/x/net/context"
	// "google.golang.org/grpc"
	// "google.golang.org/grpc/grpclog"
	// "io"
	"time"
	// "net"
	// "sync"
	SYS "syscall"
)

const bulkCnt = 100

func main() {
	fmt.Println("Starting main")
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

	// go func() {

	// 	for {
	// 		fmt.Println("pushing data")
	// 		err := runPushCliV2()
	// 		if err != nil {
	// 			log.Error("Failed to push sleeping")
	// 			time.Sleep(2 * time.Second)
	// 		}
	// 	}
	// }()

	// go func() {
	// 	runPushKV()
	// }()

	// go func() {
	// 	cli := CLI.NewClient()
	// 	defer cli.Close()
	// 	for {
	// 		now := time.Now()
	// 		k, err := cli.QueryKey("woo2", "woo")
	// 		if err != nil {
	// 			fmt.Println("query key error: ", err)
	// 		}

	// 		duration := time.Since(now)
	// 		fmt.Println("query key latency: ", duration, ", result: ", string(k))
	// 		time.Sleep(2 * time.Second)
	// 	}

	// }()

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
			fmt.Println("Querying data")
			start := time.Now().Add(-720 * time.Hour)
			end := time.Now()
			repo := "name0"
			partitions, err := getPartitions(repo, start, end)
			if err != nil {
				log.Error("getPartitions: ", err)
				time.Sleep(2 * time.Second)
				continue
			}

			for _, path := range partitions {

				func(partitionPath string) {
					cli := CLI.NewClient()
					t := time.Now()
					it := time.Now()
					c, err := cli.QueryPartition(
						pb.QueryPartition{
							PartitionPath: partitionPath,
							QueryIndexed: &pb.QueryIndexed{
								Repo:      repo,
								StartTime: start.Format(time.RFC3339),
								EndTime:   end.Format(time.RFC3339),
								BatchSize: 5000,
							},
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
						if cnt%2000 == 0 {
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
		}
	}()

	// go func() {

	// 	for {
	// 		fmt.Println("Querying data")
	// 		start := time.Now().Add(-720 * time.Hour)
	// 		end := time.Now()
	// 		repo := "name0"
	// 		partitions, err := getPartitions(repo, start, end)
	// 		if err != nil {
	// 			log.Error("getPartitions: ", err)
	// 			time.Sleep(2 * time.Second)
	// 			continue
	// 		}

	// 		wg := &sync.WaitGroup{}
	// 		for _, path := range partitions {

	// 			wg.Add(1)
	// 			go func(partitionPath string) {
	// 				cli := CLI.NewClient()
	// 				defer wg.Done()
	// 				t := time.Now()
	// 				it := time.Now()
	// 				c, err := cli.QueryPartition(
	// 					pb.QueryPartition{
	// 						PartitionPath: partitionPath,
	// 						QueryIndexed: &pb.QueryIndexed{
	// 							Repo:      repo,
	// 							StartTime: start.Format(time.RFC3339),
	// 							EndTime:   end.Format(time.RFC3339),
	// 							BatchSize: 100,
	// 						},
	// 					}, 10)
	// 				if err != nil {
	// 					log.Error("QueryPartitions: ", err)
	// 					time.Sleep(2 * time.Second)
	// 					return
	// 				}
	// 				cnt := 0
	// 				rcds := 0
	// 				sampRcds := 0
	// 				for r := range c {
	// 					if r.Err != nil {
	// 						log.Error("query error: ", r.Err)
	// 						continue
	// 					}
	// 					rcds += len(r.R)
	// 					sampRcds += len(r.R)
	// 					if cnt%2000 == 0 {
	// 						if len(r.R) > 20 {
	// 							log.Info("0: ", string(r.R[0]))
	// 							log.Info("10: ", string(r.R[10]))
	// 							duration := time.Since(it)
	// 							log.Info("read rate/s: ", float64(sampRcds)/duration.Seconds())
	// 							it = time.Now()
	// 							sampRcds = 0
	// 						}
	// 					}
	// 					cnt++
	// 				}
	// 				duration := time.Since(t)
	// 				log.Info("read msgs: ", cnt, " rcds: ", rcds, " took: ", duration)
	// 			}(path)
	// 		}
	// 		wg.Wait()

	// 	}
	// }()

	death.WaitForDeath()
	log.Info("shutdown")
}

func getPartitions(repo string, startDate, endDate time.Time) ([]string, error) {
	cli := CLI.NewClient()
	return cli.GetPartitions(pb.PartitionsRequest{Repo: repo, StartTime: startDate.Format(time.RFC3339), EndTime: endDate.Format(time.RFC3339)})
}

func setupRepo() {
	for {
		cli := CLI.NewClient()
		err := cli.UpdateRepository(pb.RepoSettings{
			Repo:              "name0",
			Format:            pb.RepoSettings_JSON,
			TimeField:         "normalDate",
			GroupByFields:     []string{0: "id"},
			GroupByPartitions: 32,
			TTL:               7,
		},
		)
		fmt.Println("finished updateRepo call")
		if err == nil {
			fmt.Println("done setting up repo")
			break
		} else {
			log.Error("failed to update settings: ", err)
			time.Sleep(2 * time.Second)
		}
	}

}

func runPushCliV2() error {
	cli := CLI.NewClient()
	defer cli.Close()

	cnt := 100000
	msgs := randomJson(cnt)
	now := time.Now()
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

func runQueryKV() {
	cli := CLI.NewClient()
	now := time.Now()
	cnt := 0
	for {
		err := cli.WriteKV("woo2", uuid.NewV4().String(), []byte(uuid.NewV4().String()))
		if err != nil {
			fmt.Println("error writing key: ", err)
			fmt.Println("sleeping...")
			time.Sleep(2 * time.Second)
		} else {
			cnt++
		}
		if cnt >= 1000 {
			duration := time.Since(now)
			log.Info("keys: ", cnt, " in ", duration, " rate: ", float64(cnt)/duration.Seconds())
			now = time.Now()
			cnt = 0
		}
	}
}

func runPushKV() error {
	cli := CLI.NewClient()
	now := time.Now()
	cnt := 0
	for {
		pairs := makeKVs("woo2", 10000)
		err := cli.WriteKVPairs("woo2", pairs...)
		if err != nil {
			fmt.Println("error writing key: ", err)
			fmt.Println("sleeping...")
			time.Sleep(2 * time.Second)
		} else {
			cnt += len(pairs)
		}
		if cnt >= 1000 {
			duration := time.Since(now)
			log.Info("keys: ", cnt, " in ", duration, " rate: ", float64(cnt)/duration.Seconds())
			now = time.Now()
			cnt = 0
		}
	}
}

func makeKVs(repo string, cnt int) []*pb.KVPair {
	pairs := make([]*pb.KVPair, 0, cnt)
	for i := 0; i < cnt; i++ {
		pairs = append(pairs, &pb.KVPair{Key: uuid.NewV4().String(), Value: []byte(uuid.NewV4().String())})
	}
	return pairs

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
			pr.Row = append(pr.Row, row)
		}
		data = append(data, pr)
	}
	return data
}
