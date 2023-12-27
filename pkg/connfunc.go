package pkg

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
	stan "github.com/nats-io/stan.go"
)

// Подключение к кластеру NATS Streaming
func NatsConnection(clientID string) stan.Conn {
	sc, err := stan.Connect("test-cluster", clientID, stan.NatsURL("nats://localhost:4222"))
	if err != nil {
		log.Fatalf("Ошибка подключения к NATS Streaming: %v", err)
	}
	return sc
}

// Открытие соединения с бд
func DatabaseConnection() *sql.DB {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=restapi_dev sslmode=disable")
	if err != nil {
		log.Fatalf("Ошибка подключения к PostgreSQL: %v", err)
	}
	return db
}
