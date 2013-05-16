package main

import (
	"log"
	"net"
	"time"
)

const (
	comQuit byte = iota + 1
	comInitDB
	comQuery
	comFieldList
	comCreateDB
	comDropDB
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOut
	comRegiserSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
)

func main() {
	ln, err := net.Listen("tcp", ":3316")
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		go proxify(conn)
	}
}

func proxify(conn net.Conn) {
	server, err := net.Dial("tcp", "localhost:3306")
	if err != nil {
		log.Println("Could not dial server")
		log.Println(err)
		conn.Close()
	}
	go forwardWithLog(conn, server)
	go forward(server, conn)
}

func forward(src, sink net.Conn) {
	buffer := make([]byte, 16777219)
	src.SetDeadline(time.Time{})
	for {
		n, err := src.Read(buffer)
		if err != nil {
			sink.Close()
			log.Println("Could not read from source")
			log.Println(err)
			return
		}

		_, err = sink.Write(buffer[0:n])
		if err != nil {
			sink.Close()
			log.Println("Could not write to sink")
			log.Println(err)
			return
		}
	}
}

func forwardWithLog(src, sink net.Conn) {
	buffer := make([]byte, 16777219)
	src.SetDeadline(time.Time{})
	for {
		n, err := src.Read(buffer)
		if err != nil {
			sink.Close()
			log.Println("Could not read from source")
			log.Println(err)
			return
		}

		switch buffer[4] {
		case comQuery:
			log.Printf("Query: %s\n", string(buffer[5:n]))
		case comStmtPrepare:
			log.Printf("Prepare Query: %s\n", string(buffer[5:n]))
		}

		switch buffer[11] {
		case comQuery:
			log.Printf("Query: %s\n", string(buffer[12:n]))
		case comStmtPrepare:
			log.Printf("Prepare Query: %s\n", string(buffer[12:n]))
		}

		_, err = sink.Write(buffer[0:n])
		if err != nil {
			sink.Close()
			log.Println("Could not write to sink")
			log.Println(err)
			return
		}
	}

}
