package util

import (
	"context"
	"database/sql"
)

func GetDbConnection(db *sql.DB) *sql.Conn {
	conn, err := db.Conn(context.Background())
	if err != nil {
		panic("failed to acquire connection")
	}
	return conn
}

func CloseDbConnection(conn *sql.Conn) {
	err := conn.Close()
	if err != nil {
		panic("failed to close connection: " + err.Error())
	}
}
