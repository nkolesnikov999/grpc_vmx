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

	"github.com/openconfig/gnmi/proto/gnmi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	gnmiPort = ":57400"

	// Sample intervals in minutes - change this value to set different intervals
	SampleIntervalMinutes = 1 // 1, 2, 3, 5, 10, etc.

	// Convert minutes to nanoseconds for gNMI SampleInterval
	SampleIntervalNanoseconds = SampleIntervalMinutes * 60 * 1000000000
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

// InterfaceCounter represents the current counter state of an interface
type InterfaceCounter struct {
	OutOctets      uint64
	LastUpdate     time.Time
	PreviousTime   time.Time
	LastDeviceTime time.Time // Time from device for last update
}

// DeviceState tracks the counter state of all interfaces on a device
type DeviceState struct {
	DeviceIP string
	Counters map[string]*InterfaceCounter // interface name -> counter state
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
		Counters: make(map[string]*InterfaceCounter),
	}
	deviceStates[deviceIP] = state
	return state
}

// updateInterfaceCounter updates the counter state of an interface and returns change information
func updateInterfaceCounter(deviceIP, interfaceName string, newOutOctets uint64, updateTime time.Time) (bool, uint64, time.Duration) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	// Get or create device state
	deviceState := deviceStates[deviceIP]
	if deviceState == nil {
		deviceState = &DeviceState{
			DeviceIP: deviceIP,
			Counters: make(map[string]*InterfaceCounter),
		}
		deviceStates[deviceIP] = deviceState
	}

	// Get or create interface counter
	ifaceCounter := deviceState.Counters[interfaceName]
	if ifaceCounter == nil {
		ifaceCounter = &InterfaceCounter{
			OutOctets:      newOutOctets,
			LastUpdate:     updateTime,
			PreviousTime:   time.Time{}, // Zero time for first update
			LastDeviceTime: updateTime,
		}
		deviceState.Counters[interfaceName] = ifaceCounter
		return true, 0, 0 // First time seeing this interface
	}

	// Check for changes
	oldOutOctets := ifaceCounter.OutOctets
	changed := oldOutOctets != newOutOctets

	var timeDiff time.Duration
	if changed {
		// Calculate time difference using device timestamps
		if !ifaceCounter.LastDeviceTime.IsZero() {
			timeDiff = updateTime.Sub(ifaceCounter.LastDeviceTime)
		}

		// Update counter state
		ifaceCounter.PreviousTime = ifaceCounter.LastUpdate
		ifaceCounter.OutOctets = newOutOctets
		ifaceCounter.LastUpdate = updateTime
		ifaceCounter.LastDeviceTime = updateTime
	} else {
		// Even if no change, show time from last device update
		if !ifaceCounter.LastDeviceTime.IsZero() {
			timeDiff = updateTime.Sub(ifaceCounter.LastDeviceTime)
		}
		// Update device time even if no change
		ifaceCounter.LastDeviceTime = updateTime
	}

	return changed, oldOutOctets, timeDiff
}

// printCounterUpdate prints detailed information about interface counter updates
func printCounterUpdate(deviceIP string, prefix *gnmi.Path, u *gnmi.Update, updateTime time.Time) {
	// Extract interface name from the path
	pathStr := buildFullPath(prefix, u.Path)

	// Parse the path to extract interface name
	// Expected format: /interfaces/interface[name=ge-0/0/1]/state/counters/out-octets

	// Find interface name using regex-like approach since interface names contain slashes
	var interfaceName string

	// Look for interface[name=...] pattern
	interfaceStart := strings.Index(pathStr, "interface[name=")
	if interfaceStart != -1 {
		interfaceStart += len("interface[name=")
		interfaceEnd := strings.Index(pathStr[interfaceStart:], "]")
		if interfaceEnd != -1 {
			interfaceName = pathStr[interfaceStart : interfaceStart+interfaceEnd]
		}
	}

	// Check if this is an out-octets counter
	if !strings.HasSuffix(pathStr, "/state/counters/out-octets") {
		return // Not an out-octets counter
	}

	if interfaceName == "" {
		return // Could not parse interface name
	}

	// Get the new counter value
	var newOutOctets uint64
	switch v := u.Val.Value.(type) {
	case *gnmi.TypedValue_UintVal:
		newOutOctets = v.UintVal
	case *gnmi.TypedValue_IntVal:
		if v.IntVal >= 0 {
			newOutOctets = uint64(v.IntVal)
		}
	default:
		return // Unsupported value type
	}

	// Use timestamp from gNMI SubscribeResponse (more accurate than current time)
	timestamp := updateTime

	// Update counter and check for changes
	changed, oldOutOctets, timeDiff := updateInterfaceCounter(deviceIP, interfaceName, newOutOctets, timestamp)

	// Always show counter updates (not just changes) since we want periodic updates
	timestampStr := timestamp.Format("2006-01-02 15:04:05")

	// Extract IP address from deviceIP (remove port)
	ip := strings.Split(deviceIP, ":")[0]

	// Show update with delta, time difference and rate
	var timeDiffStr string
	var rateStr string
	if timeDiff == 0 {
		timeDiffStr = "n/a"
		rateStr = "n/a"
	} else {
		timeDiffStr = fmt.Sprintf("%.0fs", timeDiff.Seconds())
		if changed {
			delta := newOutOctets - oldOutOctets
			// Calculate rate in bps: (delta_octets * 8) / time_seconds
			rate := float64(delta*8) / timeDiff.Seconds()
			rateStr = fmt.Sprintf("%.0f bps", rate)
		} else {
			rateStr = "0 bps"
		}
	}

	if changed {
		// Show change with delta, time difference and rate
		delta := newOutOctets - oldOutOctets
		changeMsg := fmt.Sprintf("📊 [%s] %s | Interface: %s | out-octets: %d (Δ%d, Δt: %s, rate: %s)",
			timestampStr, ip, interfaceName, newOutOctets, delta, timeDiffStr, rateStr)
		fmt.Println(changeMsg)
	} else {
		// Show periodic update (no change) with time difference and rate
		updateMsg := fmt.Sprintf("📊 [%s] %s | Interface: %s | out-octets: %d (Δt: %s, rate: %s)",
			timestampStr, ip, interfaceName, newOutOctets, timeDiffStr, rateStr)
		fmt.Println(updateMsg)
	}
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

// createInterfaceCounterSubscriptions creates subscriptions for out-octets counters on all ge- interfaces
func createInterfaceCounterSubscriptions(deviceIP string, interfaces []string) []*gnmi.Subscription {
	var subscriptions []*gnmi.Subscription

	logrus.Infof("Creating counter subscriptions for device %s:", deviceIP)

	// Filter only ge- interfaces
	var geInterfaces []string
	for _, iface := range interfaces {
		if strings.HasPrefix(iface, "ge-") {
			geInterfaces = append(geInterfaces, iface)
		}
	}

	logrus.Infof("  - All interfaces: %v", interfaces)
	logrus.Infof("  - GE interfaces to monitor: %v", geInterfaces)
	logrus.Infof("  - Total GE interfaces: %d", len(geInterfaces))

	for _, iface := range geInterfaces {
		// Subscribe to out-octets counter
		counterPath := createInterfaceCounterPath(iface, "out-octets")
		subscriptions = append(subscriptions, &gnmi.Subscription{
			Path:           counterPath,
			Mode:           gnmi.SubscriptionMode_SAMPLE,
			SampleInterval: SampleIntervalNanoseconds,
		})

		logrus.Debugf("  - Added counter subscription for interface %s (out-octets)", iface)
	}

	logrus.Infof("  - Total counter subscriptions created: %d", len(subscriptions))
	return subscriptions
}

// createInterfaceCounterPath creates a gNMI path for interface counter monitoring
func createInterfaceCounterPath(interfaceName, counterType string) *gnmi.Path {
	return &gnmi.Path{
		Elem: []*gnmi.PathElem{
			{Name: "interfaces"},
			{
				Name: "interface",
				Key:  map[string]string{"name": interfaceName},
			},
			{Name: "state"},
			{Name: "counters"},
			{Name: counterType},
		},
	}
}

// monitorDevice connects to a device and monitors interface statuses with retry logic
func monitorDevice(ctx context.Context, device DeviceInfo) error {
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

		// Create subscriptions for interface counters
		subscriptions := createInterfaceCounterSubscriptions(device.IP, device.Interfaces)

		// Log subscription details
		logrus.Infof("Subscription mode: STREAM (real-time updates)")
		logrus.Infof("Encoding: PROTO")
		logrus.Infof("Mode: SAMPLE (periodic updates every %d minutes)", SampleIntervalMinutes)
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
		fmt.Printf("===== Monitoring interface counters on %s =====\n", device.IP)
		logrus.Infof("Waiting for data from %s (sample interval: %d minutes)", device.IP, SampleIntervalMinutes)

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
						// Check for specific gNMI errors that might be temporary
						if s.Code() == codes.Unavailable || s.Code() == codes.Unknown {
							logrus.Warnf("Temporary connection issue for %s: %v", device.IP, err)
							break
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
						printCounterUpdate(device.IP, m.Update.Prefix, u, updateTime)
					}
				case *gnmi.SubscribeResponse_SyncResponse:
					fmt.Printf("===== Sync completed for %s =====\n", device.IP)
					// After sync, continue monitoring for updates
					logrus.Infof("Initial data collection completed for %s, continuing to monitor for changes", device.IP)
					continue
				case *gnmi.SubscribeResponse_Error:
					logrus.Errorf("Subscribe error from %s: %v", device.IP, m.Error.Message)
				default:
					// ignore other types
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
	totalGEInterfaces := 0
	for _, device := range devices {
		geCount := 0
		for _, iface := range device.Interfaces {
			if strings.HasPrefix(iface, "ge-") {
				geCount++
			}
		}
		totalInterfaces += len(device.Interfaces)
		totalGEInterfaces += geCount
		logrus.Infof("  - Device %s: %d total interfaces (%d GE interfaces)", device.IP, len(device.Interfaces), geCount)
	}
	logrus.Infof("  - Total interfaces: %d", totalInterfaces)
	logrus.Infof("  - Total GE interfaces to monitor: %d", totalGEInterfaces)
	logrus.Infof("  - Total counter subscriptions: %d (1 per GE interface)", totalGEInterfaces)

	// Initialize device states
	for _, device := range devices {
		getOrCreateDeviceState(device.IP)
	}

	fmt.Printf("Starting continuous interface counter monitoring for %d devices...\n", len(devices))
	fmt.Printf("Sample interval: %d minutes\n", SampleIntervalMinutes)
	fmt.Printf("===== Starting continuous monitoring at %s =====\n", time.Now().Format("2006-01-02 15:04:05"))

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to handle graceful shutdown
	done := make(chan error, len(devices))

	// Start monitoring for each device in separate goroutines
	for _, device := range devices {
		go func(d DeviceInfo) {
			for {
				err := monitorDevice(ctx, d)
				if err != nil && err != context.Canceled {
					logrus.Errorf("Device monitoring error for %s: %v", d.IP, err)
					// Wait before retrying
					select {
					case <-ctx.Done():
						done <- ctx.Err()
						return
					case <-time.After(30 * time.Second):
						continue
					}
				} else {
					// Normal exit or context cancelled
					done <- err
					return
				}
			}
		}(device)
	}

	// Wait for all devices to complete or error
	for i := 0; i < len(devices); i++ {
		err := <-done
		if err != nil && err != context.Canceled {
			logrus.Errorf("Device monitoring error: %v", err)
		}
	}

	fmt.Printf("===== Monitoring completed at %s =====\n", time.Now().Format("2006-01-02 15:04:05"))
}
