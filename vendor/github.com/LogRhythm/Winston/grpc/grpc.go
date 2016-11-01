package grpc

import (
	"fmt"
	log "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

//GRPC is a simple wrapper with helper functions for managing grpc connections
type GRPC struct {
	connections map[string]*grpc.ClientConn
}

//ERR_DOES_NOT_EXIST...
var ERR_DOES_NOT_EXIST = fmt.Errorf("connection does not exist in map")

//NewGRPC...
func NewGRPC() GRPC {
	g := GRPC{connections: make(map[string]*grpc.ClientConn)}
	return g
}

//Connection returns a connection for a given url, allowing you to reuse grpc connections
func (g GRPC) Connection(url string) (con *grpc.ClientConn, err error) {
	// fmt.Println("CON CNT: ", len(f.connections.C))
	con, ok := g.connections[url]
	if !ok {
		con, err = grpc.Dial(url, grpc.WithInsecure())
		if err != nil {
			return con, err
		}
		g.connections[url] = con
	}
	return con, err
}

//CloseConnection will close a single connection, erroring if it does not exist or the close fails
func (g GRPC) CloseConnection(url string) error {
	c, ok := g.connections[url]
	if !ok {
		return ERR_DOES_NOT_EXIST
	}
	return c.Close()
}

//Close all connections
func (g GRPC) Close() error {
	var lastErr error
	for _, c := range g.connections {
		err := c.Close()
		if err != nil {
			log.Error("Failed to close connection: ", err)
			lastErr = err
		}
	}
	return lastErr
}
