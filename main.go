package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
	csvFile  = "interface_data.csv"
)

type InterfaceData struct {
	Timestamp   time.Time
	Name        string
	OperStatus  string
	AdminStatus string
	InOctets    uint64
	OutOctets   uint64
	InPkts      uint64
	OutPkts     uint64
	Speed       uint64
	MTU         uint32
}

// last known aggregated values for consolidated CSV writes
var latest InterfaceData
var lastEmitted InterfaceData
var PushInterval = 1 // minutes
var configuredInterval = time.Duration(PushInterval) * time.Minute
var currentSlot time.Time
var slotAggregate InterfaceData
var lastSlotTimestamp time.Time
var lastKnownCounters = struct {
	InOctets  uint64
	OutOctets uint64
	InPkts    uint64
	OutPkts   uint64
}{}
var useSlotFlush = false
var lastFlushTime time.Time

// L2 members aggregation for ae1
type MemberCounters struct {
	InOctets  uint64
	OutOctets uint64
	InPkts    uint64
	OutPkts   uint64
}

var memberCounters = map[string]*MemberCounters{
	"ge-0/0/1": {},
	"ge-0/0/6": {},
}
var slotMemberCounters = map[string]*MemberCounters{
	"ge-0/0/1": {},
	"ge-0/0/6": {},
}

// basicAuth реализует gRPC credentials.PerRPCCredentials для базовой аутентификации
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

func (b *basicAuth) RequireTransportSecurity() bool {
	return false
}

// contains проверяет, содержит ли строка подстроку
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// checkGNMIServer удален как неиспользуемый

func main() {
	// Настройка логирования
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	logrus.Info("Запуск gNMI мониторинга интерфейса ae1.0")

	// Создание CSV файла с заголовками
	if err := createCSVFile(); err != nil {
		logrus.Fatalf("Ошибка создания CSV файла: %v", err)
	}

	// Создание gRPC соединения с аутентификацией и таймаутом
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDial()
	conn, err := grpc.DialContext(dialCtx, deviceIP,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(&basicAuth{
			username: username,
			password: password,
		}),
		grpc.WithBlock(),
	)
	if err != nil {
		logrus.Fatalf("Ошибка подключения к устройству: %v", err)
	}
	defer conn.Close()

	// Создание gNMI клиента
	client := gnmi.NewGNMIClient(conn)

	// Создание контекста с отменой
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Пропускаем предварительную проверку сервера из-за проблем с кодировкой
	logrus.Info("🚀 Переходим к подписке на интерфейс ae1.0...")

	// Обработка сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logrus.Info("Получен сигнал завершения, останавливаем мониторинг...")
		cancel()
	}()

	// Запуск мониторинга
	if err := startMonitoring(ctx, client); err != nil {
		logrus.Fatalf("Ошибка мониторинга: %v", err)
	}

	logrus.Info("Мониторинг завершен")
}

func createCSVFile() error {
	// Пишем заголовок только если файл новый или пустой
	info, err := os.Stat(csvFile)
	if os.IsNotExist(err) {
		file, err := os.Create(csvFile)
		if err != nil {
			return err
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		headers := []string{
			"Timestamp",
			"Interface Name",
			"Operational Status",
			"Administrative Status",
			"Input Octets",
			"Output Octets",
			"Input Packets",
			"Output Packets",
			"Speed (bps)",
			"MTU",
		}
		if err := writer.Write(headers); err != nil {
			return err
		}
		writer.Flush()
		return writer.Error()
	}
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		file, err := os.OpenFile(csvFile, os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		headers := []string{
			"Timestamp",
			"Interface Name",
			"Operational Status",
			"Administrative Status",
			"Input Octets",
			"Output Octets",
			"Input Packets",
			"Output Packets",
			"Speed (bps)",
			"MTU",
		}
		if err := writer.Write(headers); err != nil {
			return err
		}
		writer.Flush()
		return writer.Error()
	}
	return nil
}

func startMonitoring(ctx context.Context, client gnmi.GNMIClient) error {
	updateCount := 0
	// Создание подписки
	subscribeRequest := &gnmi.SubscribeRequest{
		Request: &gnmi.SubscribeRequest_Subscribe{
			Subscribe: &gnmi.SubscriptionList{
				Mode:     gnmi.SubscriptionList_STREAM,
				Encoding: gnmi.Encoding_JSON,
				Subscription: []*gnmi.Subscription{
					// Statuses and MTU at interface level (1 minute)
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ae1"}}, {Name: "state"}, {Name: "oper-status"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ae1"}}, {Name: "state"}, {Name: "admin-status"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ae1"}}, {Name: "state"}, {Name: "mtu"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					// Logical interface ae1.0 counters container
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ae1.0"}}, {Name: "state"}, {Name: "counters"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					// L2 subinterface(0) counters for member ge-0/0/1 (1 minute)
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/1"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "in-octets"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/1"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "out-octets"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/1"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "in-pkts"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/1"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "out-pkts"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					// L2 subinterface(0) counters for member ge-0/0/6 (1 minute)
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/6"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "in-octets"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/6"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "out-octets"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/6"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "in-pkts"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ge-0/0/6"}}, {Name: "subinterfaces"}, {Name: "subinterface", Key: map[string]string{"index": "0"}}, {Name: "state"}, {Name: "counters"}, {Name: "out-pkts"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					// Ethernet speed on interface ae1 (1 minute)
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ae1"}}, {Name: "ethernet"}, {Name: "state"}, {Name: "negotiated-port-speed"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
					{Path: &gnmi.Path{Elem: []*gnmi.PathElem{{Name: "interfaces"}, {Name: "interface", Key: map[string]string{"name": "ae1"}}, {Name: "ethernet"}, {Name: "state"}, {Name: "port-speed"}}}, Mode: gnmi.SubscriptionMode_SAMPLE, SampleInterval: uint64(time.Minute.Nanoseconds())},
				},
			},
		},
	}

	// Применяем интервал подписки из PushInterval для всех SAMPLE-подписок
	sampleIntervalNs := uint64(configuredInterval.Nanoseconds())
	if s := subscribeRequest.GetSubscribe(); s != nil {
		for _, sub := range s.Subscription {
			if sub.Mode == gnmi.SubscriptionMode_SAMPLE {
				sub.SampleInterval = sampleIntervalNs
				// Логируем применённый интервал для наглядности
				logrus.Infof("Подписка %s: SampleInterval=%dns", pathToString(sub.Path), sub.SampleInterval)
			}
		}
	}

	// Создание потока подписки
	stream, err := client.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("ошибка создания потока подписки: %v", err)
	}

	// Отправка запроса подписки
	if err := stream.Send(subscribeRequest); err != nil {
		return fmt.Errorf("ошибка отправки запроса подписки: %v", err)
	}

	logrus.Info("Запрос подписки отправлен, ожидание подтверждения...")

	// Ожидание первого ответа для подтверждения подписки
	response, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("ошибка получения подтверждения подписки: %v", err)
	}

	// Проверка типа ответа
	switch resp := response.Response.(type) {
	case *gnmi.SubscribeResponse_SyncResponse:
		logrus.Info("✅ Подписка успешно активирована! Получен SyncResponse")
		logrus.Infof("SyncResponse: %v", resp.SyncResponse)
	case *gnmi.SubscribeResponse_Update:
		logrus.Info("✅ Подписка успешно активирована! Получены данные")
		logrus.Infof("Первое обновление получено в %v", time.Unix(0, resp.Update.Timestamp))
		// Обрабатываем первое обновление
		if err := processNotification(response); err != nil {
			logrus.Errorf("Ошибка обработки первого уведомления: %v", err)
		}
	case *gnmi.SubscribeResponse_Error:
		return fmt.Errorf("❌ Ошибка подписки: %v", resp.Error)
	default:
		logrus.Warnf("⚠️  Неожиданный тип ответа: %T", resp)
	}

	// Обработка входящих уведомлений
	for {
		select {
		case <-ctx.Done():
			// Флашим текущий слот при завершении
			_ = flushCurrentSlot()
			return nil
		default:
			response, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					_ = flushCurrentSlot()
					return nil // Контекст отменен
				}
				return fmt.Errorf("ошибка получения данных: %v", err)
			}

			if err := processNotification(response); err != nil {
				logrus.Errorf("Ошибка обработки уведомления: %v", err)
			} else {
				updateCount++
				if updateCount%10 == 0 {
					logrus.Infof("📊 Получено %d обновлений", updateCount)
				}
				// Флаш по таймеру внутри основного цикла (fallback)
				now := time.Now()
				if lastFlushTime.IsZero() || now.Sub(lastFlushTime) >= configuredInterval {
					if err := buildAndSaveRecord(now, "loop"); err != nil {
						logrus.Errorf("Ошибка записи CSV: %v", err)
					} else {
						lastFlushTime = now
					}
				}
			}
		}
	}
}

func processNotification(response *gnmi.SubscribeResponse) error {
	if response.GetUpdate() == nil {
		return nil
	}

	notification := response.GetUpdate()
	timestamp := time.Unix(0, notification.Timestamp)

	logrus.Debugf("Получено обновление в %v", timestamp)

	// Определяем слот по интервалу и при переходе слота флашим одну строку
	slotTime := timestamp.Truncate(configuredInterval)
	if currentSlot.IsZero() {
		currentSlot = slotTime
		slotAggregate = InterfaceData{Name: "ae1.0"}
		slotMemberCounters = map[string]*MemberCounters{
			"ge-0/0/1": {},
			"ge-0/0/6": {},
		}
	}
	if useSlotFlush && slotTime.After(currentSlot) {
		// Завершаем предыдущий слот: заполняем недостающие поля и пишем одну строку
		// Агрегируем счётчики членов
		if slotAggregate.InOctets == 0 {
			slotAggregate.InOctets = slotMemberCounters["ge-0/0/1"].InOctets + slotMemberCounters["ge-0/0/6"].InOctets
		}
		if slotAggregate.OutOctets == 0 {
			slotAggregate.OutOctets = slotMemberCounters["ge-0/0/1"].OutOctets + slotMemberCounters["ge-0/0/6"].OutOctets
		}
		if slotAggregate.InPkts == 0 {
			slotAggregate.InPkts = slotMemberCounters["ge-0/0/1"].InPkts + slotMemberCounters["ge-0/0/6"].InPkts
		}
		if slotAggregate.OutPkts == 0 {
			slotAggregate.OutPkts = slotMemberCounters["ge-0/0/1"].OutPkts + slotMemberCounters["ge-0/0/6"].OutPkts
		}
		// Фолбэки из последней успешной записи
		if slotAggregate.OperStatus == "" {
			slotAggregate.OperStatus = lastEmitted.OperStatus
		}
		if slotAggregate.AdminStatus == "" {
			slotAggregate.AdminStatus = lastEmitted.AdminStatus
		}
		if slotAggregate.Speed == 0 {
			slotAggregate.Speed = lastEmitted.Speed
		}
		if slotAggregate.MTU == 0 {
			slotAggregate.MTU = lastEmitted.MTU
		}
		// Пер-полевая подстановка ненулевых значений
		slotSumInOctets := slotMemberCounters["ge-0/0/1"].InOctets + slotMemberCounters["ge-0/0/6"].InOctets
		slotSumOutOctets := slotMemberCounters["ge-0/0/1"].OutOctets + slotMemberCounters["ge-0/0/6"].OutOctets
		slotSumInPkts := slotMemberCounters["ge-0/0/1"].InPkts + slotMemberCounters["ge-0/0/6"].InPkts
		slotSumOutPkts := slotMemberCounters["ge-0/0/1"].OutPkts + slotMemberCounters["ge-0/0/6"].OutPkts

		globalSumInOctets := memberCounters["ge-0/0/1"].InOctets + memberCounters["ge-0/0/6"].InOctets
		globalSumOutOctets := memberCounters["ge-0/0/1"].OutOctets + memberCounters["ge-0/0/6"].OutOctets
		globalSumInPkts := memberCounters["ge-0/0/1"].InPkts + memberCounters["ge-0/0/6"].InPkts
		globalSumOutPkts := memberCounters["ge-0/0/1"].OutPkts + memberCounters["ge-0/0/6"].OutPkts

		// InOctets
		if slotAggregate.InOctets == 0 {
			slotAggregate.InOctets = chooseNonZeroUint64(slotSumInOctets, chooseNonZeroUint64(lastKnownCounters.InOctets, chooseNonZeroUint64(globalSumInOctets, lastEmitted.InOctets)))
		}
		// OutOctets
		if slotAggregate.OutOctets == 0 {
			slotAggregate.OutOctets = chooseNonZeroUint64(slotSumOutOctets, chooseNonZeroUint64(lastKnownCounters.OutOctets, chooseNonZeroUint64(globalSumOutOctets, lastEmitted.OutOctets)))
		}
		// InPkts
		if slotAggregate.InPkts == 0 {
			slotAggregate.InPkts = chooseNonZeroUint64(slotSumInPkts, chooseNonZeroUint64(lastKnownCounters.InPkts, chooseNonZeroUint64(globalSumInPkts, lastEmitted.InPkts)))
		}
		// OutPkts
		if slotAggregate.OutPkts == 0 {
			slotAggregate.OutPkts = chooseNonZeroUint64(slotSumOutPkts, chooseNonZeroUint64(lastKnownCounters.OutPkts, chooseNonZeroUint64(globalSumOutPkts, lastEmitted.OutPkts)))
		}
		if !lastSlotTimestamp.IsZero() {
			slotAggregate.Timestamp = lastSlotTimestamp
		} else {
			slotAggregate.Timestamp = currentSlot
		}
		logrus.Infof("FLUSH slot %s: InOctets=%d OutOctets=%d InPkts=%d OutPkts=%d",
			slotAggregate.Timestamp.Format("15:04:05"), slotAggregate.InOctets, slotAggregate.OutOctets, slotAggregate.InPkts, slotAggregate.OutPkts)
		if err := saveToCSV(&slotAggregate); err != nil {
			return err
		}
		lastEmitted = slotAggregate

		// Готовим новый слот
		currentSlot = slotTime
		slotAggregate = InterfaceData{Name: "ae1.0"}
		slotMemberCounters = map[string]*MemberCounters{
			"ge-0/0/1": {},
			"ge-0/0/6": {},
		}
		lastSlotTimestamp = time.Time{}
	}

	// Парсинг данных интерфейса
	interfaceData := &InterfaceData{
		Timestamp: timestamp,
		Name:      "ae1.0",
	}

	// Извлечение данных из уведомления
	countersChanged := false
	for _, notifUpdate := range notification.Update {
		// Получаем полный путь включая префикс
		fullPath := buildFullPath(notification.Prefix, notifUpdate.Path)
		logrus.Debugf("Получен полный путь: %s", fullPath)

		// Оставляем только данные по интерфейсу ae1 или логическому ae1.0
		if !(contains(fullPath, "[name=ae1]") || contains(fullPath, "[name=ae1.0]") || contains(fullPath, "[name=ge-0/0/1]") || contains(fullPath, "[name=ge-0/0/6]")) {
			logrus.Debugf("Пропускаем путь (не ae1/ae1.0): %s", fullPath)
			continue
		}

		logrus.Debugf("Обрабатываем путь для ae1: %s", fullPath)

		// Пробуем разные типы значений (с сохранением нулей)
		var value string
		var uintVal uint64
		var mtuSet bool

		tv := notifUpdate.Val
		if tv == nil {
			logrus.Debugf("Пустое значение для пути: %s", fullPath)
			continue
		}
		switch v := tv.Value.(type) {
		case *gnmi.TypedValue_StringVal:
			value = v.StringVal
			logrus.Debugf("DATA %s = %s", fullPath, value)
			// map speed enums if applicable
			if contains(fullPath, "port-speed") {
				switch value {
				case "SPEED_1GB":
					latest.Speed = 1_000_000_000
				case "SPEED_2_5GB", "SPEED_2500MB":
					latest.Speed = 2_500_000_000
				case "SPEED_10GB":
					latest.Speed = 10_000_000_000
				case "SPEED_100GB":
					latest.Speed = 100_000_000_000
				default:
					latest.Speed = 0
				}
			}
		case *gnmi.TypedValue_UintVal:
			uintVal = v.UintVal
			logrus.Debugf("DATA %s = %d", fullPath, uintVal)
			if contains(fullPath, "mtu") {
				interfaceData.MTU = uint32(uintVal)
				latest.MTU = uint32(uintVal)
				mtuSet = true
			}
		case *gnmi.TypedValue_IntVal:
			uintVal = uint64(v.IntVal)
			logrus.Debugf("DATA %s = %d", fullPath, uintVal)
			if contains(fullPath, "mtu") {
				interfaceData.MTU = uint32(uintVal)
				latest.MTU = uint32(uintVal)
				mtuSet = true
			}
		case *gnmi.TypedValue_FloatVal:
			leaf := ""
			if notifUpdate.Path != nil && len(notifUpdate.Path.Elem) > 0 {
				leaf = notifUpdate.Path.Elem[len(notifUpdate.Path.Elem)-1].Name
			}
			logrus.Debugf("DATA %s = %.0f", fullPath, v.FloatVal)
			switch leaf {
			case "in-forwarded-pkts", "in-unicast-pkts", "in-pkts":
				latest.InPkts = uint64(v.FloatVal)
				countersChanged = true
			case "out-forwarded-pkts", "out-unicast-pkts", "out-pkts":
				latest.OutPkts = uint64(v.FloatVal)
				countersChanged = true
			case "in-forwarded-octets", "in-octets":
				latest.InOctets = uint64(v.FloatVal)
				countersChanged = true
			case "out-forwarded-octets", "out-octets":
				latest.OutOctets = uint64(v.FloatVal)
				countersChanged = true
			case "mtu":
				latest.MTU = uint32(v.FloatVal)
				interfaceData.MTU = uint32(v.FloatVal)
				mtuSet = true
			}
		case *gnmi.TypedValue_JsonIetfVal:
			var any interface{}
			if err := json.Unmarshal(v.JsonIetfVal, &any); err == nil {
				switch m := any.(type) {
				case map[string]interface{}:
					if enc, err := json.Marshal(m); err == nil {
						logrus.Debugf("DATA %s = %s", fullPath, string(enc))
					}
					if x, ok := m["in-octets"].(float64); ok {
						latest.InOctets = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InOctets = uint64(x)
							lastKnownCounters.InOctets = uint64(x)
						}
					}
					if x, ok := m["out-octets"].(float64); ok {
						latest.OutOctets = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutOctets = uint64(x)
							lastKnownCounters.OutOctets = uint64(x)
						}
					}
					if x, ok := m["in-unicast-pkts"].(float64); ok {
						latest.InPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InPkts = uint64(x)
							lastKnownCounters.InPkts = uint64(x)
						}
					}
					if x, ok := m["out-unicast-pkts"].(float64); ok {
						latest.OutPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutPkts = uint64(x)
							lastKnownCounters.OutPkts = uint64(x)
						}
					}
					if x, ok := m["in-pkts"].(float64); ok {
						latest.InPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InPkts = uint64(x)
							lastKnownCounters.InPkts = uint64(x)
						}
					}
					if x, ok := m["out-pkts"].(float64); ok {
						latest.OutPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutPkts = uint64(x)
							lastKnownCounters.OutPkts = uint64(x)
						}
					}
					if x, ok := m["in-forwarded-pkts"].(float64); ok {
						latest.InPkts = uint64(x)
						countersChanged = true
					}
					if x, ok := m["out-forwarded-pkts"].(float64); ok {
						latest.OutPkts = uint64(x)
						countersChanged = true
					}
					if x, ok := m["in-forwarded-octets"].(float64); ok {
						latest.InOctets = uint64(x)
						countersChanged = true
					}
					if x, ok := m["out-forwarded-octets"].(float64); ok {
						latest.OutOctets = uint64(x)
						countersChanged = true
					}
				case float64:
					leaf := ""
					if notifUpdate.Path != nil && len(notifUpdate.Path.Elem) > 0 {
						leaf = notifUpdate.Path.Elem[len(notifUpdate.Path.Elem)-1].Name
					}
					logrus.Debugf("DATA %s = %.0f", fullPath, m)
					switch leaf {
					case "in-forwarded-pkts", "in-unicast-pkts", "in-pkts":
						latest.InPkts = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InPkts = uint64(m)
							lastKnownCounters.InPkts = uint64(m)
						}
					case "out-forwarded-pkts", "out-unicast-pkts", "out-pkts":
						latest.OutPkts = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutPkts = uint64(m)
							lastKnownCounters.OutPkts = uint64(m)
						}
					case "in-forwarded-octets", "in-octets":
						latest.InOctets = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InOctets = uint64(m)
							lastKnownCounters.InOctets = uint64(m)
						}
					case "out-forwarded-octets", "out-octets":
						latest.OutOctets = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutOctets = uint64(m)
							lastKnownCounters.OutOctets = uint64(m)
						}
					case "mtu":
						latest.MTU = uint32(m)
						interfaceData.MTU = uint32(m)
						mtuSet = true
					}
				case string:
					value = m
					logrus.Debugf("DATA %s = %s", fullPath, value)
					if contains(fullPath, "port-speed") {
						switch value {
						case "SPEED_1GB":
							latest.Speed = 1_000_000_000
						case "SPEED_2_5GB", "SPEED_2500MB":
							latest.Speed = 2_500_000_000
						case "SPEED_10GB":
							latest.Speed = 10_000_000_000
						case "SPEED_100GB":
							latest.Speed = 100_000_000_000
						default:
							latest.Speed = 0
						}
					}
				}
			}
		case *gnmi.TypedValue_JsonVal:
			var any interface{}
			if err := json.Unmarshal(v.JsonVal, &any); err == nil {
				switch m := any.(type) {
				case map[string]interface{}:
					if enc, err := json.Marshal(m); err == nil {
						logrus.Debugf("DATA %s = %s", fullPath, string(enc))
					}
					if x, ok := m["in-octets"].(float64); ok {
						latest.InOctets = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InOctets = uint64(x)
							lastKnownCounters.InOctets = uint64(x)
						}
					}
					if x, ok := m["out-octets"].(float64); ok {
						latest.OutOctets = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutOctets = uint64(x)
							lastKnownCounters.OutOctets = uint64(x)
						}
					}
					if x, ok := m["in-unicast-pkts"].(float64); ok {
						latest.InPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InPkts = uint64(x)
							lastKnownCounters.InPkts = uint64(x)
						}
					}
					if x, ok := m["out-unicast-pkts"].(float64); ok {
						latest.OutPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutPkts = uint64(x)
							lastKnownCounters.OutPkts = uint64(x)
						}
					}
					if x, ok := m["in-pkts"].(float64); ok {
						latest.InPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InPkts = uint64(x)
							lastKnownCounters.InPkts = uint64(x)
						}
					}
					if x, ok := m["out-pkts"].(float64); ok {
						latest.OutPkts = uint64(x)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutPkts = uint64(x)
							lastKnownCounters.OutPkts = uint64(x)
						}
					}
					if x, ok := m["in-forwarded-pkts"].(float64); ok {
						latest.InPkts = uint64(x)
						countersChanged = true
					}
					if x, ok := m["out-forwarded-pkts"].(float64); ok {
						latest.OutPkts = uint64(x)
						countersChanged = true
					}
					if x, ok := m["in-forwarded-octets"].(float64); ok {
						latest.InOctets = uint64(x)
						countersChanged = true
					}
					if x, ok := m["out-forwarded-octets"].(float64); ok {
						latest.OutOctets = uint64(x)
						countersChanged = true
					}
					if x, ok := m["mtu"].(float64); ok {
						latest.MTU = uint32(x)
						interfaceData.MTU = uint32(x)
						mtuSet = true
					}
				case float64:
					leaf := ""
					if notifUpdate.Path != nil && len(notifUpdate.Path.Elem) > 0 {
						leaf = notifUpdate.Path.Elem[len(notifUpdate.Path.Elem)-1].Name
					}
					logrus.Debugf("DATA %s = %.0f", fullPath, m)
					switch leaf {
					case "in-forwarded-pkts", "in-unicast-pkts", "in-pkts":
						latest.InPkts = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InPkts = uint64(m)
							lastKnownCounters.InPkts = uint64(m)
						}
					case "out-forwarded-pkts", "out-unicast-pkts", "out-pkts":
						latest.OutPkts = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutPkts = uint64(m)
							lastKnownCounters.OutPkts = uint64(m)
						}
					case "in-forwarded-octets", "in-octets":
						latest.InOctets = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.InOctets = uint64(m)
							lastKnownCounters.InOctets = uint64(m)
						}
					case "out-forwarded-octets", "out-octets":
						latest.OutOctets = uint64(m)
						countersChanged = true
						if contains(fullPath, "[name=ae1.0]") {
							slotAggregate.OutOctets = uint64(m)
							lastKnownCounters.OutOctets = uint64(m)
						}
					case "mtu":
						latest.MTU = uint32(m)
						interfaceData.MTU = uint32(m)
						mtuSet = true
					}
				case string:
					value = m
					logrus.Debugf("DATA %s = %s", fullPath, value)
					if contains(fullPath, "port-speed") {
						switch value {
						case "SPEED_1GB":
							latest.Speed = 1_000_000_000
						case "SPEED_2_5GB", "SPEED_2500MB":
							latest.Speed = 2_500_000_000
						case "SPEED_10GB":
							latest.Speed = 10_000_000_000
						case "SPEED_100GB":
							latest.Speed = 100_000_000_000
						default:
							latest.Speed = 0
						}
					}
				}
			}
		case *gnmi.TypedValue_BoolVal:
			logrus.Debugf("DATA %s = %t", fullPath, v.BoolVal)
		default:
			logrus.Debugf("Неизвестный тип значения для пути: %s", fullPath)
			continue
		}

		switch {
		case contains(fullPath, "oper-status"):
			interfaceData.OperStatus = value
			latest.OperStatus = value
			slotAggregate.OperStatus = value
			logrus.Debugf("Установлен oper-status: %s", value)
		case contains(fullPath, "admin-status"):
			interfaceData.AdminStatus = value
			latest.AdminStatus = value
			slotAggregate.AdminStatus = value
			logrus.Debugf("Установлен admin-status: %s", value)
		case contains(fullPath, "subinterfaces/subinterface[index=0]/state/counters") && contains(fullPath, "[name=ge-0/0/1]"):
			// также учитываем IPv4 counters
			fallthrough
		case contains(fullPath, "subinterfaces/subinterface[index=0]/ipv4/state/counters") && contains(fullPath, "[name=ge-0/0/1]"):
			// Извлекаем числовое значение из tv (поддержка Uint/Int/Float)
			numericVal := uint64(0)
			switch vv := tv.Value.(type) {
			case *gnmi.TypedValue_UintVal:
				numericVal = vv.UintVal
			case *gnmi.TypedValue_IntVal:
				if vv.IntVal < 0 {
					numericVal = 0
				} else {
					numericVal = uint64(vv.IntVal)
				}
			case *gnmi.TypedValue_FloatVal:
				if vv.FloatVal < 0 {
					numericVal = 0
				} else {
					numericVal = uint64(vv.FloatVal)
				}
			}
			if contains(fullPath, "in-octets") {
				updateMonotonic(&memberCounters["ge-0/0/1"].InOctets, numericVal, "in-octets", "ge-0/0/1")
				updateMonotonic(&slotMemberCounters["ge-0/0/1"].InOctets, numericVal, "in-octets", "ge-0/0/1")
				countersChanged = true
			}
			if contains(fullPath, "out-octets") {
				updateMonotonic(&memberCounters["ge-0/0/1"].OutOctets, numericVal, "out-octets", "ge-0/0/1")
				updateMonotonic(&slotMemberCounters["ge-0/0/1"].OutOctets, numericVal, "out-octets", "ge-0/0/1")
				countersChanged = true
			}
			if contains(fullPath, "in-pkts") {
				updateMonotonic(&memberCounters["ge-0/0/1"].InPkts, numericVal, "in-pkts", "ge-0/0/1")
				updateMonotonic(&slotMemberCounters["ge-0/0/1"].InPkts, numericVal, "in-pkts", "ge-0/0/1")
				countersChanged = true
			}
			if contains(fullPath, "out-pkts") {
				updateMonotonic(&memberCounters["ge-0/0/1"].OutPkts, numericVal, "out-pkts", "ge-0/0/1")
				updateMonotonic(&slotMemberCounters["ge-0/0/1"].OutPkts, numericVal, "out-pkts", "ge-0/0/1")
				countersChanged = true
			}
			// Поддерживаем slotAggregate и lastKnownCounters
			slotAggregate.InOctets = slotMemberCounters["ge-0/0/1"].InOctets + slotMemberCounters["ge-0/0/6"].InOctets
			slotAggregate.OutOctets = slotMemberCounters["ge-0/0/1"].OutOctets + slotMemberCounters["ge-0/0/6"].OutOctets
			slotAggregate.InPkts = slotMemberCounters["ge-0/0/1"].InPkts + slotMemberCounters["ge-0/0/6"].InPkts
			slotAggregate.OutPkts = slotMemberCounters["ge-0/0/1"].OutPkts + slotMemberCounters["ge-0/0/6"].OutPkts
			// Обновляем глобальные последние известные суммы
			lastKnownCounters.InOctets = memberCounters["ge-0/0/1"].InOctets + memberCounters["ge-0/0/6"].InOctets
			lastKnownCounters.OutOctets = memberCounters["ge-0/0/1"].OutOctets + memberCounters["ge-0/0/6"].OutOctets
			lastKnownCounters.InPkts = memberCounters["ge-0/0/1"].InPkts + memberCounters["ge-0/0/6"].InPkts
			lastKnownCounters.OutPkts = memberCounters["ge-0/0/1"].OutPkts + memberCounters["ge-0/0/6"].OutPkts
		case contains(fullPath, "subinterfaces/subinterface[index=0]/state/counters") && contains(fullPath, "[name=ge-0/0/6]"):
			// также учитываем IPv4 counters
			fallthrough
		case contains(fullPath, "subinterfaces/subinterface[index=0]/ipv4/state/counters") && contains(fullPath, "[name=ge-0/0/6]"):
			// Извлекаем числовое значение из tv (поддержка Uint/Int/Float)
			numericVal := uint64(0)
			switch vv := tv.Value.(type) {
			case *gnmi.TypedValue_UintVal:
				numericVal = vv.UintVal
			case *gnmi.TypedValue_IntVal:
				if vv.IntVal < 0 {
					numericVal = 0
				} else {
					numericVal = uint64(vv.IntVal)
				}
			case *gnmi.TypedValue_FloatVal:
				if vv.FloatVal < 0 {
					numericVal = 0
				} else {
					numericVal = uint64(vv.FloatVal)
				}
			}
			if contains(fullPath, "in-octets") {
				updateMonotonic(&memberCounters["ge-0/0/6"].InOctets, numericVal, "in-octets", "ge-0/0/6")
				updateMonotonic(&slotMemberCounters["ge-0/0/6"].InOctets, numericVal, "in-octets", "ge-0/0/6")
				countersChanged = true
			}
			if contains(fullPath, "out-octets") {
				updateMonotonic(&memberCounters["ge-0/0/6"].OutOctets, numericVal, "out-octets", "ge-0/0/6")
				updateMonotonic(&slotMemberCounters["ge-0/0/6"].OutOctets, numericVal, "out-octets", "ge-0/0/6")
				countersChanged = true
			}
			if contains(fullPath, "in-pkts") {
				updateMonotonic(&memberCounters["ge-0/0/6"].InPkts, numericVal, "in-pkts", "ge-0/0/6")
				updateMonotonic(&slotMemberCounters["ge-0/0/6"].InPkts, numericVal, "in-pkts", "ge-0/0/6")
				countersChanged = true
			}
			if contains(fullPath, "out-pkts") {
				updateMonotonic(&memberCounters["ge-0/0/6"].OutPkts, numericVal, "out-pkts", "ge-0/0/6")
				updateMonotonic(&slotMemberCounters["ge-0/0/6"].OutPkts, numericVal, "out-pkts", "ge-0/0/6")
				countersChanged = true
			}
			// Поддерживаем slotAggregate и lastKnownCounters
			slotAggregate.InOctets = slotMemberCounters["ge-0/0/1"].InOctets + slotMemberCounters["ge-0/0/6"].InOctets
			slotAggregate.OutOctets = slotMemberCounters["ge-0/0/1"].OutOctets + slotMemberCounters["ge-0/0/6"].OutOctets
			slotAggregate.InPkts = slotMemberCounters["ge-0/0/1"].InPkts + slotMemberCounters["ge-0/0/6"].InPkts
			slotAggregate.OutPkts = slotMemberCounters["ge-0/0/1"].OutPkts + slotMemberCounters["ge-0/0/6"].OutPkts
			// Обновляем глобальные последние известные суммы
			lastKnownCounters.InOctets = memberCounters["ge-0/0/1"].InOctets + memberCounters["ge-0/0/6"].InOctets
			lastKnownCounters.OutOctets = memberCounters["ge-0/0/1"].OutOctets + memberCounters["ge-0/0/6"].OutOctets
			lastKnownCounters.InPkts = memberCounters["ge-0/0/1"].InPkts + memberCounters["ge-0/0/6"].InPkts
			lastKnownCounters.OutPkts = memberCounters["ge-0/0/1"].OutPkts + memberCounters["ge-0/0/6"].OutPkts
		case contains(fullPath, "in-octets"):
			interfaceData.InOctets = uintVal
			latest.InOctets = uintVal
			countersChanged = true
			logrus.Debugf("Установлен in-octets: %d", uintVal)
		case contains(fullPath, "out-octets"):
			interfaceData.OutOctets = uintVal
			latest.OutOctets = uintVal
			countersChanged = true
			logrus.Debugf("Установлен out-octets: %d", uintVal)
		case contains(fullPath, "in-pkts") || contains(fullPath, "in-unicast-pkts"):
			interfaceData.InPkts = uintVal
			latest.InPkts = uintVal
			countersChanged = true
			logrus.Debugf("Установлен in-pkts: %d", uintVal)
		case contains(fullPath, "out-pkts") || contains(fullPath, "out-unicast-pkts"):
			interfaceData.OutPkts = uintVal
			latest.OutPkts = uintVal
			countersChanged = true
			logrus.Debugf("Установлен out-pkts: %d", uintVal)
		case contains(fullPath, "mtu"):
			if !mtuSet {
				interfaceData.MTU = uint32(uintVal)
				latest.MTU = uint32(uintVal)
				slotAggregate.MTU = uint32(uintVal)
				logrus.Debugf("Установлен mtu (fallback): %d", uintVal)
			}
		default:
			logrus.Debugf("Неизвестное поле: %s", fullPath)
		}
	}

	// Агрегируем L2-счётчики членов в итог для ae1.0
	latest.InOctets = memberCounters["ge-0/0/1"].InOctets + memberCounters["ge-0/0/6"].InOctets
	latest.OutOctets = memberCounters["ge-0/0/1"].OutOctets + memberCounters["ge-0/0/6"].OutOctets
	latest.InPkts = memberCounters["ge-0/0/1"].InPkts + memberCounters["ge-0/0/6"].InPkts
	latest.OutPkts = memberCounters["ge-0/0/1"].OutPkts + memberCounters["ge-0/0/6"].OutPkts

	// Обновляем агрегат текущего слота
	slotAggregate.Name = "ae1.0"
	slotAggregate.OperStatus = chooseNonEmpty(slotAggregate.OperStatus, latest.OperStatus)
	slotAggregate.AdminStatus = chooseNonEmpty(slotAggregate.AdminStatus, latest.AdminStatus)
	if slotAggregate.Speed == 0 {
		slotAggregate.Speed = latest.Speed
	}
	if slotAggregate.MTU == 0 {
		slotAggregate.MTU = latest.MTU
	}
	// Если логические счётчики отсутствуют, подставим сумму членов
	if slotAggregate.InOctets == 0 {
		slotAggregate.InOctets = slotMemberCounters["ge-0/0/1"].InOctets + slotMemberCounters["ge-0/0/6"].InOctets
	}
	if slotAggregate.OutOctets == 0 {
		slotAggregate.OutOctets = slotMemberCounters["ge-0/0/1"].OutOctets + slotMemberCounters["ge-0/0/6"].OutOctets
	}
	if slotAggregate.InPkts == 0 {
		slotAggregate.InPkts = slotMemberCounters["ge-0/0/1"].InPkts + slotMemberCounters["ge-0/0/6"].InPkts
	}
	if slotAggregate.OutPkts == 0 {
		slotAggregate.OutPkts = slotMemberCounters["ge-0/0/1"].OutPkts + slotMemberCounters["ge-0/0/6"].OutPkts
	}
	lastSlotTimestamp = timestamp

	// Обновим "последние известные" счётчики для фолбэка на границе слота
	if latest.InOctets != 0 || latest.OutOctets != 0 || latest.InPkts != 0 || latest.OutPkts != 0 {
		lastKnownCounters.InOctets = latest.InOctets
		lastKnownCounters.OutOctets = latest.OutOctets
		lastKnownCounters.InPkts = latest.InPkts
		lastKnownCounters.OutPkts = latest.OutPkts
	}

	// Всегда записываем строку без пропусков
	latest.Timestamp = timestamp
	latest.Name = interfaceData.Name
	// Если текущая нотификация не содержит счетчиков и агрегат нулевой,
	// подставляем последние известные значения, чтобы избежать нулей в CSV
	if latest.InOctets == 0 && latest.OutOctets == 0 && latest.InPkts == 0 && latest.OutPkts == 0 {
		if lastEmitted.InOctets != 0 || lastEmitted.OutOctets != 0 || lastEmitted.InPkts != 0 || lastEmitted.OutPkts != 0 {
			latest.InOctets = lastEmitted.InOctets
			latest.OutOctets = lastEmitted.OutOctets
			latest.InPkts = lastEmitted.InPkts
			latest.OutPkts = lastEmitted.OutPkts
		}
	}
	// предотвращаем ошибку неиспользуемой переменной
	_ = countersChanged
	return nil
}

// выбирает непустое строковое значение
func chooseNonEmpty(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

func chooseNonZeroUint64(primary, fallback uint64) uint64 {
	if primary != 0 {
		return primary
	}
	return fallback
}

// Обновляет счётчик только если новое значение монотонно неубывает и не ноль
func updateMonotonic(current *uint64, newVal uint64, field, iface string) {
	if newVal == 0 {
		// игнорируем нулевые апдейты, чтобы не затирать актуальные значения
		return
	}
	if newVal < *current {
		// игнорируем регрессию счётчика
		return
	}
	*current = newVal
}

// Завершает текущий слот принудительно (например, при завершении программы)
func flushCurrentSlot() error {
	if currentSlot.IsZero() {
		return nil
	}
	// Пер-полевая подстановка ненулевых значений, аналогично основному флашу
	slotSumInOctets := slotMemberCounters["ge-0/0/1"].InOctets + slotMemberCounters["ge-0/0/6"].InOctets
	slotSumOutOctets := slotMemberCounters["ge-0/0/1"].OutOctets + slotMemberCounters["ge-0/0/6"].OutOctets
	slotSumInPkts := slotMemberCounters["ge-0/0/1"].InPkts + slotMemberCounters["ge-0/0/6"].InPkts
	slotSumOutPkts := slotMemberCounters["ge-0/0/1"].OutPkts + slotMemberCounters["ge-0/0/6"].OutPkts

	globalSumInOctets := memberCounters["ge-0/0/1"].InOctets + memberCounters["ge-0/0/6"].InOctets
	globalSumOutOctets := memberCounters["ge-0/0/1"].OutOctets + memberCounters["ge-0/0/6"].OutOctets
	globalSumInPkts := memberCounters["ge-0/0/1"].InPkts + memberCounters["ge-0/0/6"].InPkts
	globalSumOutPkts := memberCounters["ge-0/0/1"].OutPkts + memberCounters["ge-0/0/6"].OutPkts

	slotAggregate.Name = "ae1.0"
	slotAggregate.OperStatus = chooseNonEmpty(slotAggregate.OperStatus, lastEmitted.OperStatus)
	slotAggregate.AdminStatus = chooseNonEmpty(slotAggregate.AdminStatus, lastEmitted.AdminStatus)
	if slotAggregate.Speed == 0 {
		slotAggregate.Speed = lastEmitted.Speed
	}
	if slotAggregate.MTU == 0 {
		slotAggregate.MTU = lastEmitted.MTU
	}
	if slotAggregate.InOctets == 0 {
		slotAggregate.InOctets = chooseNonZeroUint64(slotSumInOctets, chooseNonZeroUint64(lastKnownCounters.InOctets, chooseNonZeroUint64(globalSumInOctets, lastEmitted.InOctets)))
	}
	if slotAggregate.OutOctets == 0 {
		slotAggregate.OutOctets = chooseNonZeroUint64(slotSumOutOctets, chooseNonZeroUint64(lastKnownCounters.OutOctets, chooseNonZeroUint64(globalSumOutOctets, lastEmitted.OutOctets)))
	}
	if slotAggregate.InPkts == 0 {
		slotAggregate.InPkts = chooseNonZeroUint64(slotSumInPkts, chooseNonZeroUint64(lastKnownCounters.InPkts, chooseNonZeroUint64(globalSumInPkts, lastEmitted.InPkts)))
	}
	if slotAggregate.OutPkts == 0 {
		slotAggregate.OutPkts = chooseNonZeroUint64(slotSumOutPkts, chooseNonZeroUint64(lastKnownCounters.OutPkts, chooseNonZeroUint64(globalSumOutPkts, lastEmitted.OutPkts)))
	}

	if !lastSlotTimestamp.IsZero() {
		slotAggregate.Timestamp = lastSlotTimestamp
	} else {
		slotAggregate.Timestamp = currentSlot
	}
	logrus.Infof("FLUSH slot (shutdown) %s: InOctets=%d OutOctets=%d InPkts=%d OutPkts=%d",
		slotAggregate.Timestamp.Format("15:04:05"), slotAggregate.InOctets, slotAggregate.OutOctets, slotAggregate.InPkts, slotAggregate.OutPkts)
	if err := saveToCSV(&slotAggregate); err != nil {
		return err
	}
	lastEmitted = slotAggregate
	return nil
}

// Собирает запись из текущих агрегатов и сохраняет
func buildAndSaveRecord(ts time.Time, reason string) error {
	rec := InterfaceData{Name: "ae1.0"}
	rec.Timestamp = ts
	rec.OperStatus = chooseNonEmpty(slotAggregate.OperStatus, lastEmitted.OperStatus)
	rec.AdminStatus = chooseNonEmpty(slotAggregate.AdminStatus, lastEmitted.AdminStatus)
	rec.Speed = chooseNonZeroUint64(slotAggregate.Speed, lastEmitted.Speed)
	if slotAggregate.MTU != 0 {
		rec.MTU = slotAggregate.MTU
	} else {
		rec.MTU = lastEmitted.MTU
	}
	sumInOctets := memberCounters["ge-0/0/1"].InOctets + memberCounters["ge-0/0/6"].InOctets
	sumOutOctets := memberCounters["ge-0/0/1"].OutOctets + memberCounters["ge-0/0/6"].OutOctets
	sumInPkts := memberCounters["ge-0/0/1"].InPkts + memberCounters["ge-0/0/6"].InPkts
	sumOutPkts := memberCounters["ge-0/0/1"].OutPkts + memberCounters["ge-0/0/6"].OutPkts
	// Обновим lastKnownCounters из текущих сумм, если те ненулевые
	if sumInOctets != 0 {
		lastKnownCounters.InOctets = sumInOctets
	}
	if sumOutOctets != 0 {
		lastKnownCounters.OutOctets = sumOutOctets
	}
	if sumInPkts != 0 {
		lastKnownCounters.InPkts = sumInPkts
	}
	if sumOutPkts != 0 {
		lastKnownCounters.OutPkts = sumOutPkts
	}
	// Приоритет: lastKnown (логические/суммы) -> глобальные суммы -> slotAggregate -> lastEmitted
	rec.InOctets = firstNonZero4(lastKnownCounters.InOctets, sumInOctets, slotAggregate.InOctets, lastEmitted.InOctets)
	rec.OutOctets = firstNonZero4(lastKnownCounters.OutOctets, sumOutOctets, slotAggregate.OutOctets, lastEmitted.OutOctets)
	rec.InPkts = firstNonZero4(lastKnownCounters.InPkts, sumInPkts, slotAggregate.InPkts, lastEmitted.InPkts)
	rec.OutPkts = firstNonZero4(lastKnownCounters.OutPkts, sumOutPkts, slotAggregate.OutPkts, lastEmitted.OutPkts)
	logrus.Infof("AGG: lastKnown(In:%d Out:%d IP:%d OP:%d) sums(In:%d Out:%d IP:%d OP:%d) slot(In:%d Out:%d IP:%d OP:%d)",
		lastKnownCounters.InOctets, lastKnownCounters.OutOctets, lastKnownCounters.InPkts, lastKnownCounters.OutPkts,
		sumInOctets, sumOutOctets, sumInPkts, sumOutPkts,
		slotAggregate.InOctets, slotAggregate.OutOctets, slotAggregate.InPkts, slotAggregate.OutPkts)
	logrus.Infof("FLUSH %s %s: InOctets=%d OutOctets=%d InPkts=%d OutPkts=%d",
		reason, rec.Timestamp.Format("15:04:05"), rec.InOctets, rec.OutOctets, rec.InPkts, rec.OutPkts)
	if err := saveToCSV(&rec); err != nil {
		return err
	}
	lastEmitted = rec
	return nil
}

// Первый ненулевой из четырех
func firstNonZero4(a, b, c, d uint64) uint64 {
	if a != 0 {
		return a
	}
	if b != 0 {
		return b
	}
	if c != 0 {
		return c
	}
	return d
}

func pathToString(path *gnmi.Path) string {
	if path == nil {
		return ""
	}

	result := ""
	for _, elem := range path.Elem {
		result += "/" + elem.Name
		if elem.Key != nil {
			for k, v := range elem.Key {
				result += "[" + k + "=" + v + "]"
			}
		}
	}

	return result
}

func buildFullPath(prefix *gnmi.Path, path *gnmi.Path) string {
	if prefix == nil && path == nil {
		return ""
	}

	fullPath := ""

	// Добавляем префикс
	if prefix != nil {
		fullPath += pathToString(prefix)
	}

	// Добавляем путь
	if path != nil {
		fullPath += pathToString(path)
	}

	logrus.Debugf("Построен полный путь: %s", fullPath)
	return fullPath
}

func saveToCSV(data *InterfaceData) error {
	file, err := os.OpenFile(csvFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{
		data.Timestamp.Format("2006-01-02 15:04:05.000"),
		data.Name,
		data.OperStatus,
		data.AdminStatus,
		fmt.Sprintf("%d", data.InOctets),
		fmt.Sprintf("%d", data.OutOctets),
		fmt.Sprintf("%d", data.InPkts),
		fmt.Sprintf("%d", data.OutPkts),
		fmt.Sprintf("%d", data.Speed),
		fmt.Sprintf("%d", data.MTU),
	}

	if err := writer.Write(record); err != nil {
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	logrus.Infof("Данные сохранены в CSV: %s - %s/%s (InOctets: %d, OutOctets: %d)",
		data.Timestamp.Format("15:04:05"), data.OperStatus, data.AdminStatus, data.InOctets, data.OutOctets)

	return nil
}
