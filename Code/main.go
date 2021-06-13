package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	//"sync"
	nsq "github.com/nsqio/go-nsq"
	//"github.com/streadway/amqp"
)

// User struct which contains a name
// a type and a list of social links
type Influencer struct {
	Index  string `json:"_index"`
	Type   string `json:"_type"`
	Id     string `json:"_id"`
	Source Source `json:"_source"`
}

// Social struct which contains a
// list of links
type Source struct {
	Profile  Profile `json:"profiles"`
	UpdateAt string  `json:"updated_at"`
}

// Social struct which contains a
// list of links
type Profile struct {
	Facebook SocialMedia `json:"facebook"`
	Twitter  SocialMedia `json:"twitter"`
}

// Social struct which contains a
// list of links
type SocialMedia struct {
	Id         int    `json:"id"`
	Username   string `json:"screen_name"`
	Bio        string `json:"bio"`
	Updated_at string `json:"updated_at"`
}

type NSQFacebookMessage struct {
	Id         int
	Username   string
	About      string
	Image      string
	Location   string
	Updated_at string
	Deleted_at string
}

type NSQTwitterMessage struct {
	Id            int
	Screen_name   string
	Biography     string
	Profile_image string
	Location      string
	Updated_at    string
	Deleted_At    string
}

func getFBMessages(wg sync.WaitGroup, c chan struct{}, config nsq.Config, q nsq.Consumer) []NSQFacebookMessage {
	var FBrequests []NSQFacebookMessage

	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {

		var request NSQFacebookMessage

		if err := json.Unmarshal(message.Body, &request); err != nil {
			log.Println("Error when Unmarshaling the message body, Err : ", err)
			// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
			return err
		}

		if request.Username != "" || request.Id != 0 || request.Updated_at != "" {
			FBrequests = append(FBrequests, request)
		}

		return nil
	}))
	err := q.ConnectToNSQD("127.0.0.1:4150")
	if err != nil {
		log.Panic("Could not connect")
	}
	go func() {
		wg.Wait()
		c <- struct{}{}
	}()
	timeout := time.Duration(3) * time.Second
	fmt.Printf("Wait for waitgroup (up to %s)\n", timeout)
	select {
	case <-c:
		fmt.Printf("Wait group finished\n")
	case <-time.After(timeout):
		fmt.Printf("Timed out waiting for wait group\n")
	}

	return FBrequests
}

func getTwitterMessages(wg sync.WaitGroup, c chan struct{}, config nsq.Config, q nsq.Consumer) []NSQTwitterMessage {
	var TWrequests []NSQTwitterMessage

	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {

		var request NSQTwitterMessage

		if err := json.Unmarshal(message.Body, &request); err != nil {
			log.Println("Error when Unmarshaling the message body, Err : ", err)
			return err
		}

		if request.Screen_name != "" || request.Id != 0 || request.Updated_at != "" {
			TWrequests = append(TWrequests, request)
		}

		return nil
	}))

	err := q.ConnectToNSQD("127.0.0.1:4150")
	if err != nil {
		log.Panic("Could not connect")
	}
	go func() {
		wg.Wait()
		c <- struct{}{}
	}()
	timeout := time.Duration(3) * time.Second
	fmt.Printf("Wait for waitgroup (up to %s)\n", timeout)
	select {
	case <-c:
		fmt.Printf("Wait group finished\n")
	case <-time.After(timeout):
		fmt.Printf("Timed out waiting for wait group\n")
	}

	return TWrequests

}

func GetDateAndTime(d string) []int {

	var date []int

	UpdateAtString := []rune(d)

	year, err := strconv.Atoi(string(UpdateAtString[0:4]))
	if err != nil {
		log.Panic("Could not connect")
	}

	month, err := strconv.Atoi(string(UpdateAtString[5:7]))
	if err != nil {
		log.Panic("Could not connect")
	}

	day, err := strconv.Atoi(string(UpdateAtString[8:10]))
	if err != nil {
		log.Panic("Could not connect")
	}

	hour, err := strconv.Atoi(string(UpdateAtString[11:13]))
	if err != nil {
		log.Panic("Could not connect")
	}

	min, err := strconv.Atoi(string(UpdateAtString[14:16]))
	if err != nil {
		log.Panic("Could not connect")
	}

	sec, err := strconv.Atoi(string(UpdateAtString[17:19]))
	if err != nil {
		log.Panic("Could not connect")
	}

	date = append(date, year)
	date = append(date, month)
	date = append(date, day)
	date = append(date, hour)
	date = append(date, min)
	date = append(date, sec)

	return date
}

func CompareDates(request []int, database []int) bool {
	//Checks Year

	for i := 1; i < len(request); i++ {

		if request[i] == database[i] {
			continue
		} else if request[i] > database[i] {
			return true
		} else {
			break
		}
	}

	return false
}

func ProcessFBmessage(influencer []Influencer, FBrequests []NSQFacebookMessage, TWrequests []NSQTwitterMessage, TWMap map[string]int) {

	for i := 0; i < len(FBrequests); i++ {
		found := false
		for x := 0; x < len(influencer); x++ {

			if FBrequests[i].Id == influencer[x].Source.Profile.Facebook.Id {
				found = true

				if FBrequests[i].Deleted_at == "" {
					found = true

					//Get Date values
					RequestDate := GetDateAndTime(FBrequests[i].Updated_at)
					InfluencerDate := GetDateAndTime(influencer[x].Source.Profile.Facebook.Updated_at)

					needToUpdate := CompareDates(RequestDate, InfluencerDate)

					//Update Kafka message
					if needToUpdate {
						fmt.Println("FB Needs Update: " + strconv.Itoa(FBrequests[i].Id))
					}

					_, prs := TWMap[FBrequests[i].Username]
					//fmt.Println("prs:", prs)
					if prs {
						if influencer[x].Source.Profile.Facebook.Id == 0 {
							//update kafka message
							fmt.Println("TW is needed for ID: " + strconv.Itoa(TWrequests[i].Id))
						}
					}

				} else {
					if influencer[x].Source.Profile.Twitter.Id == 0 {
						//Delete Kafka message
						fmt.Println("FB Needs Delete: " + strconv.Itoa(FBrequests[i].Id))
					} else {
						//Update Kafka message
						fmt.Println("FB Needs Update 2: " + strconv.Itoa(FBrequests[i].Id))
					}
				}
				break
			}

		}

		///Index Kafka Message
		if found == false && FBrequests[i].Deleted_at == "" {
			fmt.Println("FB Did not find: " + strconv.Itoa(FBrequests[i].Id))
		}
	}
}

func ProcessTWmessage(influencer []Influencer, TWrequests []NSQTwitterMessage, FBrequests []NSQFacebookMessage, FBMap map[string]int) {

	for i := 0; i < len(TWrequests); i++ {
		found := false
		for x := 0; x < len(influencer); x++ {

			if TWrequests[i].Id == influencer[x].Source.Profile.Twitter.Id {
				found = true

				if TWrequests[i].Deleted_At == "" {

					//Get Date values
					RequestDate := GetDateAndTime(TWrequests[i].Updated_at)
					InfluencerDate := GetDateAndTime(influencer[x].Source.Profile.Twitter.Updated_at)

					needToUpdate := CompareDates(RequestDate, InfluencerDate)

					//Update Kafka message
					if needToUpdate {
						fmt.Println("TW Needs Update: " + strconv.Itoa(TWrequests[i].Id))
					}

					_, prs := FBMap[TWrequests[i].Screen_name]
					//fmt.Println("prs:", prs)
					if prs {
						if influencer[x].Source.Profile.Facebook.Id == 0 {
							//update kafka message
							fmt.Println("FB is needed for ID: " + strconv.Itoa(TWrequests[i].Id))
						}
					}

				} else {
					if influencer[x].Source.Profile.Twitter.Id == 0 {
						//Delete Kafka message
						fmt.Println("TW Needs Delete: " + strconv.Itoa(TWrequests[i].Id))
					} else {
						//Update Kafka message
						fmt.Println("TW Needs Update 2: " + strconv.Itoa(TWrequests[i].Id))
					}
				}
				break
			}

		}

		///Index Kafka Message
		if found == false && TWrequests[i].Deleted_At == "" {
			fmt.Println("TW Did not find: " + strconv.Itoa(TWrequests[i].Id))
		}
	}
}

func main() {

	wg := &sync.WaitGroup{}
	wg.Add(2)
	c := make(chan struct{})

	config := nsq.NewConfig()
	q, _ := nsq.NewConsumer("facebook", "ch", config)

	///Creating the Facebook Messages
	var FBrequests []NSQFacebookMessage = getFBMessages(*wg, c, *config, *q)

	FBmap := make(map[string]int)

	for i := 0; i < len(FBrequests); i++ {
		FBmap[FBrequests[i].Username] = i
	}

	///Setting up the new Consumer
	config = nsq.NewConfig()
	q, _ = nsq.NewConsumer("twitter", "ch", config)

	///Creating the Facebook Messages
	var TWrequests []NSQTwitterMessage = getTwitterMessages(*wg, c, *config, *q)
	TWmap := make(map[string]int)

	for i := 0; i < len(TWrequests); i++ {
		TWmap[TWrequests[i].Screen_name] = 1
	}

	// Open our jsonFile
	jsonFile, err := os.Open("data/influencers.json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Successfully Opened influencers.json")
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	// read our opened xmlFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)

	// we initialize our Users array
	var influencer []Influencer

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'users' which we defined above
	json.Unmarshal([]byte(byteValue), &influencer)

	//Prints Everything
	//fmt.Printf("Influencers : %+v", users)

	ProcessFBmessage(influencer, FBrequests, TWrequests, TWmap)

	ProcessTWmessage(influencer, TWrequests, FBrequests, FBmap)

}
