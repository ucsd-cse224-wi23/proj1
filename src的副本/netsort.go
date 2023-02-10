package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type Inf struct {
	Record []byte
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	_ = yaml.Unmarshal(f, &scs)

	return scs
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	// Set up as server
	ch := make(chan Inf)
	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			go startTCPServer(ch, server.Host, server.Port)
			break
		}
	}
	//fmt.Println("tcp server", curServer.Port)
	//go startTCPServer(ch, curServer.Host, curServer.Port)

	time.Sleep(200 * time.Millisecond)

	infileName := os.Args[2]
	outfileName := os.Args[3]
	readFile, err := os.Open(infileName)
	if err != nil {
		log.Println("Error opening file: ", err)
	}

	numServers := len(scs.Servers)
	//numServers := 1
	//get the total length of bits
	count := 0
	curL := numServers
	for curL > 0 {
		curL = (curL >> 1)
		count++
	}
	for {
		record := make([]byte, 101)
		_, err := readFile.Read(record[1:])
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Println("error in readFile")
				break
			}
		} else {
			record[0] = 0
		}
		sendServerId := int(partition(record[1], count-1))
		for _, server := range scs.Servers {
			if sendServerId == server.ServerId {
				startClient(server.Host, server.Port, record)
				break
			}
		}
	}
	readFile.Close()

	//signal done to other servers
	var signalDone = make([]byte, 101)
	signalDone[0] = 1
	for _, server := range scs.Servers {
		startClient(server.Host, server.Port, signalDone)
	}

	//read own records
	var data [][]byte
	numCompleted := 0
	for {
		if numCompleted == numServers {
			break
		}
		msg := <-ch
		if msg.Record[0] == 1 {
			numCompleted += 1
		} else {
			data = append(data, msg.Record[1:])
			//fmt.Println("data appened", msg.Record)
		}
	}

	//sort.Slice(recordArray, func(i, j int) bool { return string(recordArray[i][:10]) < string(recordArray[j][:10]) })
	sort.Slice(data, func(i, j int) bool {
		repeat := 0
		for repeat < 10 {
			if data[i][repeat] < data[j][repeat] {
				return true
			} else if data[i][repeat] > data[j][repeat] {
				return false
			} else {
				repeat++
			}
		}
		return true
	})
	//fmt.Println("data after sorted", data)
	var flatData []byte
	for _, row := range data {
		flatData = append(flatData, row...)
	}
	outfile, err1 := os.Create(outfileName)
	if err1 != nil {
		log.Println("cannot open the file")
	}
	defer outfile.Close()

	// Write the binary data to the file
	err = binary.Write(outfile, binary.LittleEndian, &flatData)
	if err1 != nil {
		log.Println("cannot write the file")
	}
}

func handleConnection(conn net.Conn, ch chan<- Inf) {
	buffer := make([]byte, 101)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Print("Could not read bytes: ", err)
	}
	buffer = buffer[0:n]
	ch <- Inf{buffer}
	//fmt.Println("buffer receive:", buffer)
}

func startTCPServer(ch chan<- Inf, host string, port string) {
	ln, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Println("Could not listen on network: ", err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("unable to start connection ", err)
		}
		go handleConnection(conn, ch)
	}
}

func startClient(host string, port string, record []byte) {
	var conn net.Conn
	var err error
	for i := 0; i < 10; i++ {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			//log.Println("Could not dial: ", err)
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	defer conn.Close()

	_, err = conn.Write(record)
	//fmt.Println("buffer write", record)
	if err != nil {
		log.Println("Could not write: ", err)
	}
}

func partition(buffer byte, length int) uint8 {
	firstByte := buffer
	var firstN uint8
	firstN = uint8((firstByte >> (8 - length)))
	return firstN
}
