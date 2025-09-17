package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"unicode/utf8"

	"strings"

	"github.com/openconfig/gnmi/proto/gnmi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

func decodeBytesToString(b []byte) interface{} {
	if len(b) == 0 {
		return ""
	}
	if utf8.Valid(b) {
		return string(b)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(b)
}

func tryUnwrapAny(a *anypb.Any) interface{} {
	if a == nil {
		return nil
	}
	if v := new(wrapperspb.StringValue); a.UnmarshalTo(v) == nil {
		return v.Value
	}
	if v := new(wrapperspb.BoolValue); a.UnmarshalTo(v) == nil {
		return v.Value
	}
	if v := new(wrapperspb.Int64Value); a.UnmarshalTo(v) == nil {
		return v.Value
	}
	if v := new(wrapperspb.UInt64Value); a.UnmarshalTo(v) == nil {
		return v.Value
	}
	if v := new(wrapperspb.FloatValue); a.UnmarshalTo(v) == nil {
		return v.Value
	}
	if v := new(wrapperspb.DoubleValue); a.UnmarshalTo(v) == nil {
		return v.Value
	}
	if v := new(wrapperspb.BytesValue); a.UnmarshalTo(v) == nil {
		return decodeBytesToString(v.Value)
	}
	return "any:" + a.TypeUrl
}

func decodeDecimal(d *gnmi.Decimal64) interface{} {
	if d == nil {
		return nil
	}
	// value = digits * 10^-precision
	// Render as string to avoid floating rounding surprises
	digits := d.Digits
	precision := d.Precision
	// Build string manually
	neg := digits < 0
	if neg {
		digits = -digits
	}
	s := fmt.Sprintf("%d", digits)
	if precision > 0 {
		if int(precision) >= len(s) {
			pad := int(precision) - len(s) + 1
			s = strings.Repeat("0", pad) + s
		}
		idx := len(s) - int(precision)
		s = s[:idx] + "." + s[idx:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

func printUpdate(prefix *gnmi.Path, u *gnmi.Update) {
	p := buildFullPath(prefix, u.Path)
	fmt.Printf("Path: %s\n", p)
	switch v := u.Val.Value.(type) {
	case *gnmi.TypedValue_JsonVal:
		var any interface{}
		if err := json.Unmarshal(v.JsonVal, &any); err == nil {
			pretty, _ := json.MarshalIndent(any, "", "  ")
			fmt.Printf("JSON:\n%s\n", string(pretty))
		} else {
			fmt.Printf("json decode error: %v, raw: %s\n", err, string(v.JsonVal))
		}
	case *gnmi.TypedValue_JsonIetfVal:
		var any interface{}
		if err := json.Unmarshal(v.JsonIetfVal, &any); err == nil {
			pretty, _ := json.MarshalIndent(any, "", "  ")
			fmt.Printf("JSON_IETF:\n%s\n", string(pretty))
		} else {
			fmt.Printf("json_ietf decode error: %v, raw: %s\n", err, string(v.JsonIetfVal))
		}
	case *gnmi.TypedValue_StringVal:
		fmt.Printf("string: %s\n", v.StringVal)
	case *gnmi.TypedValue_UintVal:
		fmt.Printf("uint: %d\n", v.UintVal)
	case *gnmi.TypedValue_IntVal:
		fmt.Printf("int: %d\n", v.IntVal)
	case *gnmi.TypedValue_BoolVal:
		fmt.Printf("bool: %t\n", v.BoolVal)
	case *gnmi.TypedValue_FloatVal:
		fmt.Printf("float: %f\n", v.FloatVal)
	case *gnmi.TypedValue_DoubleVal:
		fmt.Printf("double: %f\n", v.DoubleVal)
	case *gnmi.TypedValue_AsciiVal:
		fmt.Printf("ascii: %s\n", v.AsciiVal)
	case *gnmi.TypedValue_BytesVal:
		fmt.Printf("bytes: %v\n", decodeBytesToString(v.BytesVal))
	case *gnmi.TypedValue_ProtoBytes:
		fmt.Printf("proto_bytes: %v\n", decodeBytesToString(v.ProtoBytes))
	case *gnmi.TypedValue_DecimalVal:
		fmt.Printf("decimal64: %v\n", decodeDecimal(v.DecimalVal))
	case *gnmi.TypedValue_AnyVal:
		fmt.Printf("any: %v\n", tryUnwrapAny(v.AnyVal))
	case *gnmi.TypedValue_LeaflistVal:
		// Render leaf-list elements as a JSON array for readability
		arr := make([]interface{}, 0, len(v.LeaflistVal.Element))
		for _, e := range v.LeaflistVal.Element {
			if e == nil {
				arr = append(arr, "<nil-elem>")
				continue
			}
			// Debug logs removed
			if e.Value == nil {
				arr = append(arr, "<nil-value>")
				continue
			}
			switch ev := e.Value.(type) {
			case *gnmi.TypedValue_StringVal:
				arr = append(arr, ev.StringVal)
			case *gnmi.TypedValue_AsciiVal:
				arr = append(arr, ev.AsciiVal)
			case *gnmi.TypedValue_UintVal:
				arr = append(arr, ev.UintVal)
			case *gnmi.TypedValue_IntVal:
				arr = append(arr, ev.IntVal)
			case *gnmi.TypedValue_BoolVal:
				arr = append(arr, ev.BoolVal)
			case *gnmi.TypedValue_FloatVal:
				arr = append(arr, ev.FloatVal)
			case *gnmi.TypedValue_BytesVal:
				arr = append(arr, decodeBytesToString(ev.BytesVal))
			case *gnmi.TypedValue_ProtoBytes:
				arr = append(arr, decodeBytesToString(ev.ProtoBytes))
			case *gnmi.TypedValue_DoubleVal:
				arr = append(arr, ev.DoubleVal)
			case *gnmi.TypedValue_DecimalVal:
				arr = append(arr, decodeDecimal(ev.DecimalVal))
			case *gnmi.TypedValue_AnyVal:
				arr = append(arr, tryUnwrapAny(ev.AnyVal))
			case *gnmi.TypedValue_JsonVal:
				var any interface{}
				if err := json.Unmarshal(ev.JsonVal, &any); err == nil {
					arr = append(arr, any)
				} else {
					arr = append(arr, string(ev.JsonVal))
				}
			case *gnmi.TypedValue_JsonIetfVal:
				var any interface{}
				if err := json.Unmarshal(ev.JsonIetfVal, &any); err == nil {
					arr = append(arr, any)
				} else {
					arr = append(arr, string(ev.JsonIetfVal))
				}
			default:
				arr = append(arr, "<unsupported-element>")
			}
		}
		pretty, _ := json.MarshalIndent(arr, "", "  ")
		fmt.Printf("leaflist:\n%s\n", string(pretty))
	default:
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
				Encoding: gnmi.Encoding_PROTO,
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
