package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/openconfig/gnmi/proto/gnmi"
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

func (b *basicAuth) RequireTransportSecurity() bool {
	return false
}

func main() {
	fmt.Println("🔍 Тестирование подключения к gNMI серверу...")
	fmt.Printf("Адрес: %s\n", deviceIP)
	fmt.Printf("Пользователь: %s\n", username)
	fmt.Println()

	// Создание соединения
	conn, err := grpc.Dial(deviceIP,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(&basicAuth{
			username: username,
			password: password,
		}))
	if err != nil {
		log.Fatalf("❌ Ошибка подключения: %v", err)
	}
	defer conn.Close()

	fmt.Println("✅ gRPC соединение установлено")

	// Создание клиента
	client := gnmi.NewGNMIClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Тест 1: Проверка доступности сервера
	fmt.Println("\n📡 Тест 1: Проверка доступности сервера...")
	getRequest := &gnmi.GetRequest{
		Path: []*gnmi.Path{
			{
				Elem: []*gnmi.PathElem{
					{Name: "system"},
					{Name: "state"},
				},
			},
		},
		Type:     gnmi.GetRequest_STATE,
		Encoding: gnmi.Encoding_PROTO,
	}

	_, err = client.Get(ctx, getRequest)
	if err != nil {
		fmt.Printf("❌ Сервер недоступен: %v\n", err)
	} else {
		fmt.Println("✅ Сервер доступен и отвечает")
	}

	// Тест 2: Проверка интерфейса ae1.0
	fmt.Println("\n🔌 Тест 2: Проверка интерфейса ae1.0...")
	interfaceRequest := &gnmi.GetRequest{
		Path: []*gnmi.Path{
			{
				Elem: []*gnmi.PathElem{
					{Name: "interfaces"},
					{Name: "interface", Key: map[string]string{"name": "ae1.0"}},
					{Name: "state"},
				},
			},
		},
		Type:     gnmi.GetRequest_STATE,
		Encoding: gnmi.Encoding_PROTO,
	}

	response, err := client.Get(ctx, interfaceRequest)
	if err != nil {
		fmt.Printf("❌ Интерфейс ae1.0 недоступен: %v\n", err)
	} else {
		fmt.Println("✅ Интерфейс ae1.0 найден")
		fmt.Printf("📊 Получено %d уведомлений\n", len(response.Notification))
	}

	// Тест 3: Проверка подписки
	fmt.Println("\n📡 Тест 3: Проверка возможности подписки...")
	stream, err := client.Subscribe(ctx)
	if err != nil {
		fmt.Printf("❌ Ошибка создания потока подписки: %v\n", err)
		return
	}

	subscribeRequest := &gnmi.SubscribeRequest{
		Request: &gnmi.SubscribeRequest_Subscribe{
			Subscribe: &gnmi.SubscriptionList{
				Mode: gnmi.SubscriptionList_STREAM,
				Subscription: []*gnmi.Subscription{
					{
						Path: &gnmi.Path{
							Target: "",
							Elem: []*gnmi.PathElem{
								{Name: "interfaces"},
								{Name: "interface", Key: map[string]string{"name": "ae1.0"}},
								{Name: "state"},
							},
						},
						Mode: gnmi.SubscriptionMode_TARGET_DEFINED,
					},
				},
			},
		},
	}

	err = stream.Send(subscribeRequest)
	if err != nil {
		fmt.Printf("❌ Ошибка отправки запроса подписки: %v\n", err)
		return
	}

	fmt.Println("✅ Запрос подписки отправлен")

	// Ожидание ответа
	subResponse, err := stream.Recv()
	if err != nil {
		fmt.Printf("❌ Ошибка получения ответа подписки: %v\n", err)
	} else {
		fmt.Println("✅ Подписка работает!")
		switch resp := subResponse.Response.(type) {
		case *gnmi.SubscribeResponse_SyncResponse:
			fmt.Println("📡 Получен SyncResponse")
		case *gnmi.SubscribeResponse_Update:
			fmt.Println("📊 Получены данные обновления")
		case *gnmi.SubscribeResponse_Error:
			fmt.Printf("❌ Ошибка подписки: %v\n", resp.Error)
		}
	}

	fmt.Println("\n🎉 Тестирование завершено!")
}
