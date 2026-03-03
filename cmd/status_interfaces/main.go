package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"io"
	"os"

	"strings"

	"github.com/IBM/sarama"
	"github.com/openconfig/gnmi/proto/gnmi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	gnmiPort = ":57400"
)

const (
	brokerAddress = "kafka:9092"
	topicName     = "interface-status"
)

// Global state tracking for all devices
var deviceStates = make(map[string]*DeviceState)
var stateMutex sync.RWMutex

// Config represents the configuration structure
type Config struct {
	Login    string              `json:"login"`
	Password string              `json:"password"`
	Devices  map[string][]string `json:"devices"`
}

// DeviceInfo represents device connection information
type DeviceInfo struct {
	IP         string
	Username   string
	Password   string
	Interfaces []string
}

// InterfaceState represents the current state of an interface
type InterfaceState struct {
	AdminStatus string
	OperStatus  string
	LastUpdate  time.Time
}

// DeviceState tracks the state of all interfaces on a device
type DeviceState struct {
	DeviceIP string
	States   map[string]*InterfaceState // interface name -> state
}

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

// getOrCreateDeviceState gets or creates a device state tracker
func getOrCreateDeviceState(deviceIP string) *DeviceState {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	if state, exists := deviceStates[deviceIP]; exists {
		return state
	}

	state := &DeviceState{
		DeviceIP: deviceIP,
		States:   make(map[string]*InterfaceState),
	}
	deviceStates[deviceIP] = state
	return state
}

// updateInterfaceState updates the state of an interface and returns change information
func updateInterfaceState(deviceIP, interfaceName, statusType, newStatus string, updateTime time.Time) (bool, string, string) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	// Get or create device state
	deviceState := deviceStates[deviceIP]
	if deviceState == nil {
		deviceState = &DeviceState{
			DeviceIP: deviceIP,
			States:   make(map[string]*InterfaceState),
		}
		deviceStates[deviceIP] = deviceState
	}

	// Get or create interface state
	ifaceState := deviceState.States[interfaceName]
	if ifaceState == nil {
		ifaceState = &InterfaceState{
			AdminStatus: "",
			OperStatus:  "",
			LastUpdate:  updateTime,
		}
		deviceState.States[interfaceName] = ifaceState
	}

	// Update the appropriate status and check for changes
	var oldStatus string
	var changed bool

	if statusType == "admin-status" {
		oldStatus = ifaceState.AdminStatus
		if oldStatus != newStatus {
			changed = true
			ifaceState.AdminStatus = newStatus
			ifaceState.LastUpdate = updateTime
		}
	} else if statusType == "oper-status" {
		oldStatus = ifaceState.OperStatus
		if oldStatus != newStatus {
			changed = true
			ifaceState.OperStatus = newStatus
			ifaceState.LastUpdate = updateTime
		}
	}

	return changed, oldStatus, newStatus
}

// printStatusChange prints detailed information about interface status changes
func printStatusChange(producer sarama.SyncProducer, deviceIP string, prefix *gnmi.Path, u *gnmi.Update, updateTime time.Time) {
	// Extract interface name and status type from the path
	pathStr := buildFullPath(prefix, u.Path)

	// Debug: print all received paths (commented out for production)
	// fmt.Printf("DEBUG: Received path: %s\n", pathStr)

	// Parse the path to extract interface name and status type
	// Expected format: /interfaces/interface[name=ge-0/0/1]/state/admin-status

	// Find interface name using regex-like approach since interface names contain slashes
	var interfaceName, statusType string

	// Look for interface[name=...] pattern
	interfaceStart := strings.Index(pathStr, "interface[name=")
	if interfaceStart != -1 {
		interfaceStart += len("interface[name=")
		interfaceEnd := strings.Index(pathStr[interfaceStart:], "]")
		if interfaceEnd != -1 {
			interfaceName = pathStr[interfaceStart : interfaceStart+interfaceEnd]
		}
	}

	// Look for status type at the end
	if strings.HasSuffix(pathStr, "/admin-status") {
		statusType = "admin-status"
	} else if strings.HasSuffix(pathStr, "/oper-status") {
		statusType = "oper-status"
	}

	if interfaceName == "" || statusType == "" {
		// Debug: Could not parse interface name or status type
		// fmt.Printf("DEBUG: Could not parse interface name='%s' or status type='%s' from path: %s\n", interfaceName, statusType, pathStr)
		return // Could not parse interface name or status type
	}

	// Get the new status value
	var newStatus string
	switch v := u.Val.Value.(type) {
	case *gnmi.TypedValue_StringVal:
		newStatus = v.StringVal
	case *gnmi.TypedValue_AsciiVal:
		newStatus = v.AsciiVal
	default:
		// Debug: Unsupported value type
		// fmt.Printf("DEBUG: Unsupported value type for interface %s: %T\n", interfaceName, v)
		return // Unsupported value type
	}

	// Use timestamp from gNMI SubscribeResponse (more accurate than current time)
	timestamp := updateTime

	// Update state and check for changes
	changed, oldStatus, _ := updateInterfaceState(deviceIP, interfaceName, statusType, newStatus, timestamp)

	if changed {
		// Only show actual changes, not duplicate notifications
		// Use timestamp from gNMI update for more accurate timing
		timestampStr := timestamp.Format("2006-01-02 15:04:05")
		changeMsg := fmt.Sprintf("🔄 [%s] %s | Interface: %s | %s: %s → %s",
			timestampStr, deviceIP, interfaceName, statusType, oldStatus, newStatus)
		fmt.Println(changeMsg)
		sendMessageToKafka(producer, changeMsg)
	}
	// Remove the else block to avoid showing duplicate status messages
}

// loadConfig loads configuration from config.json file
func loadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// getDeviceInfo converts config to device info slice
func getDeviceInfo(config *Config) []DeviceInfo {
	var devices []DeviceInfo
	for ip, interfaces := range config.Devices {
		devices = append(devices, DeviceInfo{
			IP:         ip + gnmiPort,
			Username:   config.Login,
			Password:   config.Password,
			Interfaces: interfaces,
		})
	}
	return devices
}

// createInterfaceStatusSubscriptions creates subscriptions for admin-status and oper-status
func createInterfaceStatusSubscriptions(deviceIP string, interfaces []string) []*gnmi.Subscription {
	var subscriptions []*gnmi.Subscription

	logrus.Infof("Creating subscriptions for device %s:", deviceIP)
	logrus.Infof("  - Interfaces to monitor: %v", interfaces)
	logrus.Infof("  - Total interfaces: %d", len(interfaces))

	for _, iface := range interfaces {
		// Subscribe to admin-status
		adminPath := createInterfacePath(iface, "admin-status")
		subscriptions = append(subscriptions, &gnmi.Subscription{
			Path: adminPath,
			Mode: gnmi.SubscriptionMode_TARGET_DEFINED,
		})

		// Subscribe to oper-status
		operPath := createInterfacePath(iface, "oper-status")
		subscriptions = append(subscriptions, &gnmi.Subscription{
			Path: operPath,
			Mode: gnmi.SubscriptionMode_TARGET_DEFINED,
		})

		logrus.Debugf("  - Added subscriptions for interface %s (admin-status, oper-status)", iface)
	}

	logrus.Infof("  - Total subscriptions created: %d (2 per interface)", len(subscriptions))
	return subscriptions
}

// createInterfacePath creates a gNMI path for interface status monitoring
func createInterfacePath(interfaceName, statusType string) *gnmi.Path {
	return &gnmi.Path{
		Elem: []*gnmi.PathElem{
			{Name: "interfaces"},
			{
				Name: "interface",
				Key:  map[string]string{"name": interfaceName},
			},
			{Name: "state"},
			{Name: statusType},
		},
	}
}

// monitorDevice connects to a device and monitors interface statuses with retry logic
func monitorDevice(ctx context.Context, device DeviceInfo, producer sarama.SyncProducer) error {
	const maxRetries = 5
	const retryDelay = 10 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logrus.Infof("Connecting to device: %s (attempt %d/%d)", device.IP, attempt, maxRetries)

		// Connect to device
		conn, err := grpc.Dial(device.IP,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(&basicAuth{username: device.Username, password: device.Password}))
		if err != nil {
			logrus.Errorf("Connection attempt %d failed for %s: %v", attempt, device.IP, err)
			if attempt < maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retryDelay):
					continue
				}
			}
			return fmt.Errorf("failed to connect to %s after %d attempts: %w", device.IP, maxRetries, err)
		}
		defer conn.Close()

		client := gnmi.NewGNMIClient(conn)

		// Create subscriptions for interface statuses
		subscriptions := createInterfaceStatusSubscriptions(device.IP, device.Interfaces)

		// Log subscription details
		logrus.Infof("Subscription mode: STREAM (real-time updates)")
		logrus.Infof("Encoding: PROTO")
		logrus.Infof("Sending subscription request to %s...", device.IP)

		// Create subscribe request with STREAM mode for real-time updates
		subReq := &gnmi.SubscribeRequest{
			Request: &gnmi.SubscribeRequest_Subscribe{
				Subscribe: &gnmi.SubscriptionList{
					Mode:         gnmi.SubscriptionList_STREAM,
					Encoding:     gnmi.Encoding_PROTO,
					Subscription: subscriptions,
				},
			},
		}

		stream, err := client.Subscribe(ctx)
		if err != nil {
			logrus.Errorf("Failed to create subscribe stream for %s: %v", device.IP, err)
			if attempt < maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retryDelay):
					continue
				}
			}
			return fmt.Errorf("failed to create subscribe stream for %s: %w", device.IP, err)
		}

		if err := stream.Send(subReq); err != nil {
			logrus.Errorf("Failed to send subscribe request to %s: %v", device.IP, err)
			if attempt < maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retryDelay):
					continue
				}
			}
			return fmt.Errorf("failed to send subscribe request to %s: %w", device.IP, err)
		}

		logrus.Infof("Successfully sent subscription request to %s", device.IP)
		fmt.Printf("===== Monitoring interface statuses on %s =====\n", device.IP)

		// Process subscription responses
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("===== Stopping monitoring for %s (context cancelled) =====\n", device.IP)
				return ctx.Err()
			default:
				resp, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						fmt.Printf("===== Connection closed for %s (EOF) =====\n", device.IP)
						return nil
					}
					if s, ok := status.FromError(err); ok {
						if s.Code() == codes.DeadlineExceeded || s.Code() == codes.Canceled {
							fmt.Printf("===== Stopping monitoring for %s (%s) =====\n", device.IP, s.Code().String())
							return nil
						}
					}
					// Connection error, retry
					logrus.Errorf("Connection error for %s: %v", device.IP, err)
					break
				}

				// Process the response
				switch m := resp.Response.(type) {
				case *gnmi.SubscribeResponse_Update:
					// Get timestamp from SubscribeResponse (more accurate than current time)
					var updateTime time.Time
					if m.Update.Timestamp > 0 {
						// gNMI timestamp is in nanoseconds since Unix epoch
						updateTime = time.Unix(0, m.Update.Timestamp)
					} else {
						// Fallback to current time if no timestamp
						updateTime = time.Now()
					}
					for _, u := range m.Update.Update {
						printStatusChange(producer, device.IP, m.Update.Prefix, u, updateTime)
					}
				case *gnmi.SubscribeResponse_SyncResponse:
					fmt.Printf("===== Sync completed for %s =====\n", device.IP)
				case *gnmi.SubscribeResponse_Error:
					logrus.Errorf("Subscribe error from %s: %v", device.IP, m.Error.Message)
				default:
					// ignore other types
				}
			}
		}

		// If we reach here, there was an error, retry if possible
		if attempt < maxRetries {
			logrus.Infof("Retrying connection to %s in %v...", device.IP, retryDelay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
				continue
			}
		}
	}

	return fmt.Errorf("failed to connect to %s after %d attempts", device.IP, maxRetries)
}

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

func newSyncProducer(brokerList []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func sendMessageToKafka(producer sarama.SyncProducer, message string) {
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder(message),
	}

	logrus.Infof("sending message to topic %s: %s", topicName, message)

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		logrus.Printf("failed to send message to Kafka: %v", err)
		return
	}

	logrus.Printf("message successfully sent to partition %d with offset %d", partition, offset)
}

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	// Load configuration
	configPath := "config.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	config, err := loadConfig(configPath)
	if err != nil {
		logrus.Fatalf("Failed to load configuration: %v", err)
	}

	devices := getDeviceInfo(config)
	if len(devices) == 0 {
		logrus.Fatal("No devices found in configuration")
	}

	// Log configuration summary
	logrus.Infof("Configuration loaded successfully:")
	logrus.Infof("  - Username: %s", config.Login)
	logrus.Infof("  - Total devices: %d", len(devices))

	totalInterfaces := 0
	for _, device := range devices {
		totalInterfaces += len(device.Interfaces)
		logrus.Infof("  - Device %s: %d interfaces", device.IP, len(device.Interfaces))
	}
	logrus.Infof("  - Total interfaces to monitor: %d", totalInterfaces)
	logrus.Infof("  - Total subscriptions: %d (2 per interface)", totalInterfaces*2)

	// Initialize device states
	for _, device := range devices {
		getOrCreateDeviceState(device.IP)
	}

	fmt.Printf("Starting interface status monitoring for %d devices...\n", len(devices))

	producer, err := newSyncProducer([]string{brokerAddress})
	if err != nil {
		logrus.Fatalf("failed to start producer: %v", err)
	}

	defer func() {
		if err = producer.Close(); err != nil {
			logrus.Fatalf("failed to close producer: %v", err)
		}
	}()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to handle graceful shutdown
	done := make(chan error, len(devices))

	// Start monitoring for each device in separate goroutines
	for _, device := range devices {
		go func(d DeviceInfo) {
			err := monitorDevice(ctx, d, producer)
			done <- err
		}(device)
	}

	// Wait for all devices to complete or error
	for i := 0; i < len(devices); i++ {
		err := <-done
		if err != nil && err != context.Canceled {
			logrus.Errorf("Device monitoring error: %v", err)
		}
	}

	fmt.Println("===== All device monitoring completed =====")
}
