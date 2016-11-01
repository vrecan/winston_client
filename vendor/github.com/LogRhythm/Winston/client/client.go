package client

import (
	"fmt"
	GRPC "github.com/LogRhythm/Winston/grpc"
	HASH "github.com/LogRhythm/Winston/hash"
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	"github.com/tidwall/gjson"
	DISC "github.schq.secious.com/UltraViolet/Discover"
	"golang.org/x/net/context"
	"io"
	"time"
)

//Client...
type Client struct {
	disc *DISC.Discover
	grpc GRPC.GRPC
}

var ERR_NOT_IMPLEMENTED = fmt.Errorf("not implemented")
var ERR_NO_WINSTONS = fmt.Errorf("no winstons found")
var ERR_NO_REPOSITORY_NAME = fmt.Errorf("no repository name")

const (
	SERVICE_NAME = "winston"
	MonthDayYear = "01-02-2006"
)

//QueryResults are the results returned over the channel from the query methods
type QueryResults struct {
	R   [][]byte
	Err error
}

//NewClient gives you a new winston client.
func NewClient() Client {
	disc, err := DISC.NewDiscover()
	if err != nil {
		panic(fmt.Sprintf("NewClient - Failed to initialize discover: %v", err))
	}
	c := Client{disc: disc, grpc: GRPC.NewGRPC()}
	return c
}

//RepositorySettings...
func (c Client) RepositorySettings(repo string) (settings *pb.RepoSettings, err error) {
	client, err := c.getPBClient()
	if err != nil {
		return settings, err
	}
	return c.repositorySettings(client, repo)
}

func (c Client) repositorySettings(client pb.V1Client, repo string) (settings *pb.RepoSettings, err error) {
	return client.GetRepoSettings(context.Background(), &pb.RepoRequest{Repo: repo})
}

//UpdateRepository will insert or update a repo with the provided settings.
func (c Client) UpdateRepository(settings pb.RepoSettings) error {
	client, err := c.getPBClient()
	if err != nil {
		return err
	}
	if len(settings.Repo) == 0 {
		return ERR_NO_REPOSITORY_NAME
	}
	_, err = client.UpsertRepo(context.Background(), &settings)
	return err
}

func (c Client) getWinstons() (winstons []string, err error) {
	urls, err := c.disc.ServiceURLSCache(SERVICE_NAME)
	if err != nil {
		return winstons, err
	}
	if len(urls) == 0 {
		return winstons, ERR_NO_WINSTONS
	}
	return urls, err
}

func (c Client) getPBClient() (pb.V1Client, error) {
	urls, err := c.disc.ServiceURLSCache(SERVICE_NAME)
	if err != nil {
		return nil, err
	}
	if len(urls) == 0 {
		return nil, ERR_NO_WINSTONS
	}
	con, err := c.grpc.Connection(urls[0])
	if err != nil {
		return nil, err
	}
	client := pb.NewV1Client(con)
	return client, err
}

//Query winston for data
func (c Client) Query(q pb.Query, bufferSize int) (chan QueryResults, error) {
	client, err := c.getPBClient()
	if err != nil {
		return nil, err
	}
	qChan := make(chan QueryResults, bufferSize)
	go func() {
		defer func() {
			close(qChan)
		}()
		stream, err := client.QueryAll(context.Background(), &q)
		if err != nil {
			qChan <- QueryResults{Err: err}
			return
		}
		for {
			data := make([][]byte, 0)
			rec, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				qChan <- QueryResults{Err: err}
				return
			}
			for _, d := range rec.Rows {
				data = append(data, d.Data)
			}
			qChan <- QueryResults{R: data}
		}
	}()
	return qChan, nil
}

func (c Client) Write(repo string, msgs [][]byte) error {
	winstons, err := c.getWinstons()
	if err != nil {
		return err
	}
	hash := HASH.NewHash()
	settings, err := c.RepositorySettings(repo)
	if err != nil {
		return err
	}
	partitionRequests := make(map[string]*pb.WritePartitionRequest, 0)
	for _, r := range msgs {
		partition := 0
		if settings.GroupByPartitions != 0 {
			var value []byte
			for _, field := range settings.GroupByFields {
				value = append(value, []byte(gjson.Get(string(r), field).String())...)
			}
			partition = hash.Hash(string(value), uint64(settings.GroupByPartitions))
		}
		//this should be conditional!!!
		t, err := getTimeFromJson(r, settings)
		if err != nil {
			log.Error("WriteStream - failed to get time from row: ", err)
			return err
		}

		partitionPath := fmt.Sprintf("%s/%s/%d", settings.Repo, t.Format(MonthDayYear), partition)
		pr, ok := partitionRequests[partitionPath]
		if !ok {
			pr = &pb.WritePartitionRequest{
				Repo:      settings.Repo,
				Partition: int32(partition),
				Time:      t.Format(time.RFC3339),
				Rows:      make([]*pb.Row, 0, 200),
			}
			partitionRequests[partitionPath] = pr
		}
		pr.Rows = append(pr.Rows, &pb.Row{Data: r, TimeNano: t.UnixNano()})
	}
	winstonCount := len(winstons)
	if winstonCount == 0 {
		return fmt.Errorf("WriteStream - no winstons to write too, possible consul issue")
	}
	for path, req := range partitionRequests {
		winstonID := hash.Hash(path, uint64(winstonCount))
		url := winstons[winstonID]
		err := c.sendToPartition(url, req)
		if nil != err {
			log.Error("WriteStream - failed to send to: ", url, " error: ", err)
			return err
		}
	}
	return nil
}

func (c Client) sendToPartition(url string, req *pb.WritePartitionRequest) error {
	con, err := c.grpc.Connection(url)
	if err != nil {
		return err
	}
	client := pb.NewV1Client(con)
	stream, err := client.XWritePartition(context.Background())
	if err != nil {
		return err
	}
	if err = stream.Send(req); err != nil {
		return err
	}
	err = c.closeWritePartitionStream(stream)
	return err
}

func (c Client) closeWritePartitionStream(stream pb.V1_XWritePartitionClient) (err error) {
	if err = stream.Send(&pb.WritePartitionRequest{TransactionDone: true}); err != nil {
		log.Error("CloseWriteStreams - write stream send: ", err)
		return err
	}
	//recv response that all writes to this point have succeeded
	_, err = stream.Recv()
	if err != nil {
		log.Error("CloseWriteStreams - Failed to get transaction done message from client: ", err)
		return err
	}
	//send final done, all transactions succeeded
	//finish transaction
	if err = stream.Send(&pb.WritePartitionRequest{TransactionDone: true}); err != nil {
		log.Error("CloseWriteStreams - write stream send: ", err)
		return err
	}
	_, err = stream.Recv()
	return err
}

//GetPartitions will return all partitions for a given date range inclusive to the day
func (c Client) GetPartitions(repo string, startDate time.Time, endDate time.Time) (partitions []string, err error) {
	client, err := c.getPBClient()
	if err != nil {
		return partitions, err
	}
	stream, err := client.GetPartitions(context.Background(), &pb.PartitionsRequest{
		Repo:      repo,
		StartTime: startDate.Format(time.RFC3339),
		EndTime:   endDate.Format(time.RFC3339),
	})
	if err != nil {
		log.Error("GetPartitions: ", err)
		return []string{}, err
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return partitions, nil
		}
		if err != nil {
			log.Error("GetPartitions: ", err)
			return partitions, err
		}
		partitions = append(partitions, in.Path)

	}
	return partitions, err
}

//Query winston for data
func (c Client) QueryPartition(q pb.QueryPartition, bufferSize int) (chan QueryResults, error) {
	winstons, err := c.getWinstons()
	qChan := make(chan QueryResults, bufferSize)
	if err != nil {
		return qChan, err
	}
	winstonCount := len(winstons)
	if winstonCount == 0 {
		return qChan, fmt.Errorf("WriteStream - no winstons to write too, possible consul issue")
	}
	hash := HASH.NewHash()
	path := fmt.Sprintf("%s/%s", q.Repo, q.PartitionPath)
	winstonID := hash.Hash(path, uint64(winstonCount))
	url := winstons[winstonID]
	con, err := c.grpc.Connection(url)
	if err != nil {
		return qChan, err
	}
	client := pb.NewV1Client(con)

	go func() {
		defer func() {
			//always close the channel on exit so that readers know we are done
			close(qChan)
		}()

		stream, err := client.XQueryPartition(context.Background(), &q)
		if err != nil {
			qChan <- QueryResults{Err: err}
			return
		}
		for {
			data := make([][]byte, 0)
			rec, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				qChan <- QueryResults{Err: err}
				return
			}
			for _, d := range rec.Rows {
				data = append(data, d.Data)
			}
			qChan <- QueryResults{R: data}
		}
	}()
	return qChan, nil
}

func getTimeFromJson(r []byte, settings *pb.RepoSettings) (t time.Time, err error) {
	if len(settings.TimeField) != 0 {
		timeValue := gjson.Get(string(r), settings.TimeField).String()
		t, err = time.Parse(time.RFC3339, timeValue)
		if err != nil {
			log.Error("getTimeFromRow - invalid timefield: ", settings.TimeField, " for repo: ", settings.Repo, " error: ", err)
			return t, fmt.Errorf("invalid time: %v in timefield: %v error: ", timeValue, settings.TimeField, err)
		}
	} else {
		t = time.Now()
	}
	return t, err
}

func (c Client) Close() error {
	return c.grpc.Close()
}
