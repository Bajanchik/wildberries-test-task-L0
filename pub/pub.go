package main

import (
	"L0/pkg"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
	stan "github.com/nats-io/stan.go"
)

func main() {
	// Подключаемся к кластеру NATS Streaming
	clientID := "test-pub-id"
	sc := pkg.NatsConnection(clientID)

	// Открываем соединение с базой данных
	db := pkg.DatabaseConnection()

	// Генерируем и отправляем случайные заказы на сервер
	ordersSending(sc, db)
}

// Генерация и отправка случайных заказов на сервер
func ordersSending(sc stan.Conn, db *sql.DB) {
	for i := 0; i < 10; i++ {
		order := generateRandomOrder()
		jsonData, err := json.Marshal(order)
		if err != nil {
			fmt.Println("Error while marshalling JSON...")
		}

		pubErr := sc.Publish("test-channel", jsonData)
		if pubErr != nil {
			fmt.Println("Проблема с отправкой данных...")
		} else {
			fmt.Printf("Данные отправлены...")
		}

		// Задержка между отправкой заказов
		time.Sleep(time.Second)
	}
}

// Генерация случайных данных для заказа
func generateRandomOrder() pkg.Order {
	orderUID := randomGenerator(19)
	trackNumber := randomGenerator(14)
	entry := randomGenerator(4)
	locale := "en"
	internalSignature := ""
	customerID := "test"
	deliveryService := "meest"
	shardKey := "9"
	smID := 99
	dateCreated := time.Now().UTC()
	oofShard := "1"

	delivery := pkg.Delivery{
		Name:    randomGenerator(11),
		Phone:   randomGenerator(11),
		Zip:     "2639809",
		City:    "Kiryat Mozkin",
		Address: "Ploshad Mira 15",
		Region:  "Kraiot",
		Email:   "test@gmail.com",
	}

	payment := pkg.Payment{
		Transaction:  orderUID,
		RequestID:    "",
		Currency:     "USD",
		Provider:     "wbpay",
		Amount:       rand.Intn(2000) + 1,
		PaymentDt:    time.Now().Unix(),
		Bank:         "alpha",
		DeliveryCost: 1500,
		GoodsTotal:   317,
		CustomFee:    0,
	}

	items := []pkg.Item{
		{
			ChrtID:      rand.Intn(9999999) + 1,
			TrackNumber: trackNumber,
			Price:       rand.Intn(2000) + 100,
			Rid:         randomGenerator(21),
			Name:        randomGenerator(8),
			Sale:        30,
			Size:        "0",
			TotalPrice:  317,
			NmID:        rand.Intn(9999999) + 1,
			Brand:       randomGenerator(13),
			Status:      202,
		},
	}

	return pkg.Order{
		OrderUID:          orderUID,
		TrackNumber:       trackNumber,
		Entry:             entry,
		Delivery:          delivery,
		Payment:           payment,
		Items:             items,
		Locale:            locale,
		InternalSignature: internalSignature,
		CustomerID:        customerID,
		DeliveryService:   deliveryService,
		ShardKey:          shardKey,
		SmID:              smID,
		DateCreated:       dateCreated,
		OofShard:          oofShard,
	}
}

// Генератор строк
func randomGenerator(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
