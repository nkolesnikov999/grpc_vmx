package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openconfig/gnmi/proto/gnmi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	deviceIP = "127.0.0.1:17081"
	username = "admin"
	password = "admin@123"
)

type basicAuth struct {
	username string
	password string
}

func (b *basicAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"username": b.username,
		"password": b.password,
	}, nil
}

func (b *basicAuth) RequireTransportSecurity() bool { return false }

func pathToString(path *gnmi.Path) string {
	if path == nil {
		return ""
	}
	res := ""
	for _, e := range path.Elem {
		res += "/" + e.Name
		if e.Key != nil {
			for k, v := range e.Key {
				res += "[" + k + "=" + v + "]"
			}
		}
	}
	return res
}

func buildFullPath(prefix *gnmi.Path, path *gnmi.Path) string {
	if prefix == nil && path == nil {
		return ""
	}
	fullPath := ""
	if prefix != nil {
		fullPath += pathToString(prefix)
	}
	if path != nil {
		fullPath += pathToString(path)
	}
	return fullPath
}

func printUpdate(prefix *gnmi.Path, u *gnmi.Update) {
	p := buildFullPath(prefix, u.Path)
	fmt.Printf("Path: %s\n", p)
	if j := u.Val.GetJsonVal(); len(j) > 0 {
		var any interface{}
		if err := json.Unmarshal(j, &any); err == nil {
			pretty, _ := json.MarshalIndent(any, "", "  ")
			fmt.Printf("JSON:\n%s\n", string(pretty))
		} else {
			fmt.Printf("json decode error: %v, raw: %s\n", err, string(j))
		}
	} else if j := u.Val.GetJsonIetfVal(); len(j) > 0 {
		var any interface{}
		if err := json.Unmarshal(j, &any); err == nil {
			pretty, _ := json.MarshalIndent(any, "", "  ")
			fmt.Printf("JSON_IETF:\n%s\n", string(pretty))
		} else {
			fmt.Printf("json_ietf decode error: %v, raw: %s\n", err, string(j))
		}
	} else if s := u.Val.GetStringVal(); s != "" {
		fmt.Printf("string: %s\n", s)
	} else if u64 := u.Val.GetUintVal(); u64 != 0 {
		fmt.Printf("uint: %d\n", u64)
	} else if i64 := u.Val.GetIntVal(); i64 != 0 {
		fmt.Printf("int: %d\n", i64)
	} else {
		fmt.Printf("(no recognizable value type)\n")
	}
}

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := grpc.Dial(deviceIP,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(&basicAuth{username: username, password: password}))
	if err != nil {
		logrus.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	client := gnmi.NewGNMIClient(conn)

	// Subscribe ONCE to /interfaces using JSON encoding
	subReq := &gnmi.SubscribeRequest{
		Request: &gnmi.SubscribeRequest_Subscribe{
			Subscribe: &gnmi.SubscriptionList{
				Mode:     gnmi.SubscriptionList_ONCE,
				Encoding: gnmi.Encoding_JSON,
				Subscription: []*gnmi.Subscription{
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}}}, Mode: gnmi.SubscriptionMode_TARGET_DEFINED},
				},
			},
		},
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		logrus.Fatalf("subscribe open error: %v", err)
	}
	if err := stream.Send(subReq); err != nil {
		logrus.Fatalf("subscribe send error: %v", err)
	}

	fmt.Println("===== Subscribe(ONCE) /interfaces =====")
	for {
		resp, err := stream.Recv()
		if err != nil {
			logrus.Fatalf("subscribe recv error: %v", err)
		}
		switch m := resp.Response.(type) {
		case *gnmi.SubscribeResponse_Update:
			for _, u := range m.Update.Update {
				printUpdate(m.Update.Prefix, u)
			}
		case *gnmi.SubscribeResponse_SyncResponse:
			fmt.Println("===== End (sync) =====")
			return
		case *gnmi.SubscribeResponse_Error:
			logrus.Fatalf("subscribe error: %v", m.Error)
		default:
			// ignore other types
		}
	}
}
