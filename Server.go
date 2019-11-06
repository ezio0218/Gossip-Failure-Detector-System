package main

import (
	"./logger"
	"net/rpc"
	"os/exec"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	//Change the INTRODUCER
	INTRODUCER = "172.22.157.12:8000"
	PERIOD     = 5000000000
	COOLDOWN   = 5000000000
)

type Member struct {
	Address        string
	HeartbeatCount int
	UpdateTime     int64
}

type Machine struct {
	IP             string
	Conn           *net.UDPConn
	MemberList     []*Member
	RecentFailList []*Member
	Alive          bool
}

type ClientRequest struct {
	Greppattern string
	Flag        string
	Server_id   string
}

type Service string

//type ClientRequest struct {
//	Greppattern string
//	Flag string
//	LogFile     string
//}

func InitMembership(ipaddress string, heartBeatCount int, updateTime int64) (member *Member) {
	member = &Member{
		UpdateTime:     updateTime,
		Address:        ipaddress,
		HeartbeatCount: heartBeatCount}
	return member
}

func startUDP() (machine *Machine) {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal("get Hostname error", err)
		os.Exit(1)
	}
	address, err := net.LookupHost(name)
	if err != nil {
		log.Fatal("get Host Address error", err)
		os.Exit(1)
	}
	host := net.JoinHostPort(address[0], "8000")
	log.Println("New Machine added with address: ", host)
	udpaddress, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		log.Fatal("Bind UDP Address error", err)
		return
	}
	conn, err := net.ListenUDP("udp", udpaddress)
	if err != nil {
		return
	}
	var list []*Member
	var list2 []*Member
	new_machine := new(Machine)
	new_machine.Conn = conn
	new_machine.IP = host
	new_machine.MemberList = list
	new_machine.RecentFailList = list2
	new_machine.Alive = true
	logger.WriteLog("UDP Server running on this local machine!")
	log.Println("UDP Server running on this local machine!")
	return new_machine
}

func (self *Machine) MemberInList(senderIP string) bool {
	for _, member := range self.MemberList {
		if senderIP == member.Address {
			return true
		}
	}
	return false
}

func (self *Machine) GetMsg() {
	if self.Alive == true {
		for {
			buf := make([]byte, 1024)
			n, addr, err := self.Conn.ReadFromUDP(buf)

			if err != nil {
				log.Panic("%d Message received from %s with error %s\n", n, addr.String(), err)
				continue
			}
			b := buf[:n]
			receive := strings.SplitN(string(b), "<CMD>", 2)
			senderIP := net.JoinHostPort(addr.IP.String(), "8000")
			if len(receive) > 1 {
				command := receive[1]
				logger.WriteLog("Command received from " + senderIP + "is" + receive[1])
				fmt.Println("Command received from %s: %s", senderIP, receive[1])
				if command == "JOIN" {
					logger.WriteLog("New Member ready to join with IP: " + senderIP)
					fmt.Println("New Member ready to join with IP: ", senderIP)
					var judge bool
					judge = self.MemberInList(senderIP)
					if judge == false {
						self.addNewMember(senderIP)
					}
				} else if command == "LEAVE" {
					logger.WriteLog("The Sender want to Leave the group, its IP is: " + senderIP)
					fmt.Println("The Sender want to Leave the group, its IP is: ", senderIP)
					self.removeMember(senderIP)
				} else if command == "REMOVE" {
					logger.WriteLog("The Sender want me to delete one process, its IP is: " + receive[0])
					fmt.Println("The Sender want me to delete one process, its IP is: " + receive[0])
					self.removeMember(receive[0])
				}
			} else {
				logger.WriteLog("Received Gossip from Sender: " + senderIP)
				new_receive := decode(string(b))
				self.MergeMemberList(new_receive)
			}
		}
	}
}

func (self *Machine) addNewMember(address string) (newMember *Member) {
	now := time.Now()
	newMember = InitMembership(address, 0, now.UnixNano())
	logger.WriteLog("Added new member: " + address)
	log.Printf("Added new member: ", address)
	self.MemberList = append(self.MemberList, newMember)
	message := encode(self.MemberList)
	for _, member := range self.MemberList {
		logger.WriteLog("Send New Member List to : " + member.Address)
		addr, err := net.ResolveUDPAddr("udp", member.Address)
		if err != nil {
			log.Panic("Error when solving Member IP when heartbeating", err)
		}
		con, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			log.Panic("Error when dialing to Gossip", err)
		}
		_, err = con.Write([]byte(message))
		if err != nil {
			log.Panic("Sending Gossiping Request Through UDP Error:", err)
		}
	}
	return
}

func (self *Machine) removeMember(address string) {
	remove_index := getIndex(address, self.MemberList)
	index := getIndex(address, self.RecentFailList)
	if index == -1 {
		self.RecentFailList = append(self.RecentFailList, self.MemberList[remove_index])}
	if remove_index != -1{
		self.MemberList = append(self.MemberList[:remove_index], self.MemberList[remove_index+1:]...)
		logger.WriteLog("Removed Address from local member list : " + address)}
}

func (self *Machine) Join() {
	var message []string
	var send_message string
	message = append(message, self.IP)
	message = append(message, "JOIN")
	send_message = strings.Replace(strings.Trim(fmt.Sprint(message), "[]"), " ", "<CMD>", -1)
	addr, err := net.ResolveUDPAddr("udp", INTRODUCER)
	logger.WriteLog("Join the network and send request to" + INTRODUCER)
	if err != nil {
		log.Panic("Error when solving Introducer IP", err)
	}
	con, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Panic("Error when dialing to Join", err)
	}
	_, err = con.Write([]byte(send_message))
	if err != nil {
		log.Panic("Sending Join Request Through UDP Error:", err)
	}
}

func (self *Machine) Leave() {
	var message []string
	var send_message string
	message = append(message, self.IP)
	message = append(message, "LEAVE")
	send_message = strings.Replace(strings.Trim(fmt.Sprint(message), "[]"), " ", "<CMD>", -1)
	for _, member := range self.MemberList {
		if member.Address == self.IP{
			continue
		}
		logger.WriteLog("Send Leave Request to the network" + member.Address)
		addr, err := net.ResolveUDPAddr("udp", member.Address)
		if err != nil {
			log.Panic("Error when solving Member IP", err)
		}
		con, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			log.Panic("Error when dialing to Leave", err)
		}
		_, err = con.Write([]byte(send_message))
		if err != nil {
			log.Panic("Sending Leave Request Through UDP Error:", err)
		}
		self.Alive = false
	}
}

func (self *Machine) Remove(address string) {
	var message []string
	var send_message string
	message = append(message, address)
	message = append(message, "REMOVE")
	send_message = strings.Replace(strings.Trim(fmt.Sprint(message), "[]"), " ", "<CMD>", -1)
	for _, member := range self.MemberList {
		if member.Address == self.IP{
			continue
		}
		logger.WriteLog("Send Remove " + address + "Request to the network" + member.Address)
		addr, err := net.ResolveUDPAddr("udp", member.Address)
		if err != nil {
			log.Panic("Error when solving Member IP", err)
		}
		con, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			log.Panic("Error when dialing to Leave", err)
		}
		_, err = con.Write([]byte(send_message))
		if err != nil {
			log.Panic("Sending Leave Request Through UDP Error:", err)
		}
	}
}

func encode(s []*Member) (vv string) {
	var d []string
	for _, ss := range s {
		sentence := string(ss.Address) + "<#INFO#>" + strconv.Itoa(ss.HeartbeatCount) + "<#INFO#>" + strconv.FormatInt(ss.UpdateTime, 10)
		d = append(d, sentence)
	}
	send_message := strings.Replace(strings.Trim(fmt.Sprint(d), "[]"), " ", "<#ENTRY#>", -1)
	return send_message
}

func decode(vv string) (s []*Member) {
	var d []*Member
	receive := strings.Split(string(vv), "<#ENTRY#>")
	for _, ss := range receive {
		sentence := strings.SplitN(string(ss), "<#INFO#>", 3)
		count, _ := strconv.Atoi(sentence[1])
		time, _ := strconv.ParseInt(sentence[2], 10, 64)
		d = append(d, InitMembership(sentence[0], count, time))
	}
	return d
}

func IsMemberExist(m *Member, s []*Member) bool {
	for _, ss := range s {
		if m.Address == ss.Address {
			return true
		}
	}
	return false
}

func GetIndex(m *Member, s []*Member) int {
	for index, ss := range s {
		if m.Address == ss.Address {
			return index
		}
	}
	return -1
}

func getIndex(m string, s []*Member) int {
	for index, ss := range s {
		if m == ss.Address {
			return index
		}
	}
	return -1
}

func (self *Machine) MergeMemberList(s []*Member) {
	if len(s) == 0 {
		return
	}
	for local_index, localmember := range self.MemberList {
		judge := IsMemberExist(localmember, s)
		if judge == false {
			continue} else if judge == true {
			index := GetIndex(localmember, s)
			if localmember.HeartbeatCount < s[index].HeartbeatCount {
				self.MemberList[local_index].HeartbeatCount = s[index].HeartbeatCount
				self.MemberList[local_index].UpdateTime = time.Now().UnixNano()
			}
		}
	}
	for _, remotemember := range s {
		judge2 := IsMemberExist(remotemember, self.MemberList)
		judge3 := IsMemberExist(remotemember, self.RecentFailList)
		if judge2 == true {
			continue
		} else if judge2 == false && judge3 == false {
			now := time.Now()
			self.MemberList = append(self.MemberList, InitMembership(remotemember.Address, remotemember.HeartbeatCount, now.UnixNano()))
		}
	}
}

func (self *Machine) updateMemberList() {
	for _, member := range self.MemberList {
		if member.Address == self.IP{
			member.UpdateTime = time.Now().UnixNano()
			member.HeartbeatCount++
		}
	}
	self.detectfailure()
	self.cleanFailList()
}

func (self *Machine) detectfailure() {
	//Postive Failure
	current := time.Now().UnixNano()
	for _, member := range self.MemberList {
		if member.Address == self.IP {
			continue
		}
		if member.HeartbeatCount <= 1 {
			continue
		} else if member.HeartbeatCount > 1 && member.UpdateTime < current-PERIOD {
			self.removeMember(member.Address)
			self.Remove(member.Address)
			logger.WriteLog("Failure Detected of: " + member.Address)
			fmt.Println("Failure Detected of: ", member.Address)
		}
	}
}

func (self *Machine) cleanFailList() {
	if len(self.RecentFailList) < 1{
		return
	}
	current := time.Now().UnixNano()
	for i:=0; i<len(self.RecentFailList); {
		if self.RecentFailList[i].UpdateTime < current - COOLDOWN {
			logger.WriteLog("RecentFailList release Address: " + self.RecentFailList[i].Address)
			self.RecentFailList = append(self.RecentFailList[:i], self.RecentFailList[i+1:]...)
		}else {
			i ++
		}
	}
}

func (self *Machine) HeartBeat() {
	if self.Alive == false {
		return
	}
	// Nobody in the list yet
	if len(self.MemberList) < 4 {
		for _, member := range(self.MemberList){
			member.UpdateTime = time.Now().UnixNano()
		}
		return
	}
	//Not Enough to get 3 neighbor,update time but do nothing
	if self.Alive == true && len(self.MemberList) >= 4 {
		var neighbor []int
		var self_index int
		self.updateMemberList()
		self_index = getIndex(self.IP, self.MemberList)
		member_list_len := len(self.MemberList)
		neighbor = append(neighbor, (self_index-1)%member_list_len)
		neighbor = append(neighbor, (self_index+1)%member_list_len)
		neighbor = append(neighbor, (self_index+2)%member_list_len)
		for index, value := range neighbor {
			if value < 0 {
				neighbor[index] = value + member_list_len
			}
		}
		message := encode(self.MemberList)

		for _, neighbor_index := range neighbor {
			logger.WriteLog("Send new Membership list to: " + self.MemberList[neighbor_index].Address)
			addr, err := net.ResolveUDPAddr("udp", self.MemberList[neighbor_index].Address)
			if err != nil {
				log.Panic("Error when solving Member IP when heartbeating", err)
			}
			con, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				log.Panic("Error when dialing to Gossip", err)
			}
			_, err = con.Write([]byte(message))
			if err != nil {
				log.Panic("Sending Gossiping Request Through UDP Error:", err)
			}
		}
	}
}

func (self *Machine) CheckInput() {
	var input string
	for {
		fmt.Scanln(&input)
		if input == "leave" {
			self.Alive = false
			self.Leave()
		}
		if input == "join" {
			self.Alive = true
			self.Join()
		}
		if input == "lsm" {
			for _, member := range self.MemberList {
				fmt.Println("Local Members are: ", member)
			}
		}
		if input == "IP" {
			fmt.Println("Local IP is: ", self.IP)

		}
	}

}

func (serv *Service) Response(query ClientRequest, res *string) error {
	err := os.Chdir("/home/yaoxiao9/group81-mp2/")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	name := "grep"
	fmt.Println("Current Server: ", query.Server_id)
	cmd := exec.Command(name, query.Flag, query.Greppattern, "Machine.log")
	fmt.Println(query.Greppattern)
	showCmd(cmd)
	out, error := cmd.Output()
	if error != nil {
		fmt.Println("Command Fails", error)
	}
	*res = strings.TrimSpace(string(out))
	return nil
}

func startTCP() {
	//
	service := new(Service)
	rpc.Register(service)
	address, error := net.ResolveTCPAddr("tcp", ":9000")
	if error != nil {
		log.Fatal("Address resolving Error: ", error)
		os.Exit(1)
	}
	listener, error := net.ListenTCP("tcp", address)
	fmt.Println("Server started")
	if error != nil {
		log.Fatal("Listening establishment Error: ", error)
		os.Exit(1)
	}
	for {
		conn, error := listener.Accept()
		if error != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
func showCmd(cmd *exec.Cmd) {
	fmt.Printf("==> Executing: %s\n", strings.Join(cmd.Args, " "))
}

func main() {
	heartbeatPeriod := 1000 * time.Millisecond
	machine := startUDP()
	machine.Join()
	time.Sleep(heartbeatPeriod)
	go machine.GetMsg()
	go machine.CheckInput()
	go startTCP()
	for {
		//Get random member , increment current members
		if machine.Alive == false {
			break
		}
		time.Sleep(heartbeatPeriod)
		go machine.HeartBeat()
	}
	machine.Leave()

}
