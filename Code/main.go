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

	//"github.com/segmentio/kafka-go"
	nsq "github.com/nsqio/go-nsq"
)

// Influencer struct which contains its
//values
type Influencer struct {
	Index  string `json:"_index"`
	Type   string `json:"_type"`
	Id     string `json:"_id"`
	Source Source `json:"_source"`
}

// Source struct which contains its
//values
type Source struct {
	Profile  Profile `json:"profiles"`
	UpdateAt string  `json:"updated_at"`
}

// Profile struct which contains its
//values
type Profile struct {
	Facebook SocialMedia `json:"facebook"`
	Twitter  SocialMedia `json:"twitter"`
}

// SocialMedia struct which contains a
// values
type SocialMedia struct {
	Id         int    `json:"id"`
	Username   string `json:"screen_name"`
	Bio        string `json:"bio"`
	Updated_at string `json:"updated_at"`
}

// Message struct which contains
// messages from the Facebook topic
type NSQFacebookMessage struct {
	Id         int
	Username   string
	About      string
	Image      string
	Location   string
	Updated_at string
	Deleted_at string
}

// Message struct which contains
// messages from the Twitter topic
type NSQTwitterMessage struct {
	Id            int
	Screen_name   string
	Biography     string
	Profile_image string
	Location      string
	Updated_at    string
	Deleted_At    string
}

// getFBMessages will retrieve the NSQ messages from the Facebook Topic
// and returns the messages in an array of FBRequests
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

// getTwitterMessages will retrieve the NSQ messages from the Twitter Topic
// and returns the messages in an array of TWRequests
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

// GetDateAndTime will recieves a string and proceeds to
// decipher it into int values for each time section and
// returns it as an array
func GetDateAndTime(d string) []int {

	var date []int

	UpdateAtString := []rune(d)

	year, err := strconv.Atoi(string(UpdateAtString[0:4]))
	if err != nil {
		log.Panic("Could not convert")
	}

	month, err := strconv.Atoi(string(UpdateAtString[5:7]))
	if err != nil {
		log.Panic("Could not convert")
	}

	day, err := strconv.Atoi(string(UpdateAtString[8:10]))
	if err != nil {
		log.Panic("Could not convert")
	}

	hour, err := strconv.Atoi(string(UpdateAtString[11:13]))
	if err != nil {
		log.Panic("Could not convert")
	}

	min, err := strconv.Atoi(string(UpdateAtString[14:16]))
	if err != nil {
		log.Panic("Could not convert")
	}

	sec, err := strconv.Atoi(string(UpdateAtString[17:19]))
	if err != nil {
		log.Panic("Could not convert")
	}

	date = append(date, year)
	date = append(date, month)
	date = append(date, day)
	date = append(date, hour)
	date = append(date, min)
	date = append(date, sec)

	return date
}

// CompareDates iterates through request array and
// see which is bigger
func CompareDates(request []int, database []int) bool {

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

//The ProcessFBmessage function process each of the Facebook NSQ messages and detects
//if we need to create index, update, or delete Kafka message
//from it
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

//The ProcessTWmessage function process each of the Twitter NSQ messages and detects
//if we need to create index, update, or delete Kafka message
//from it
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

	//Setting my timer
	wg := &sync.WaitGroup{}
	wg.Add(2)
	c := make(chan struct{})

	//Setting up the new Consumer
	config := nsq.NewConfig()
	q, _ := nsq.NewConsumer("facebook", "ch", config)

	///Creating the Facebook Messages
	var FBrequests []NSQFacebookMessage = getFBMessages(*wg, c, *config, *q)

	//Initializing the FB Map
	FBmap := make(map[string]int)

	///Setting up the FB Username Map
	for i := 0; i < len(FBrequests); i++ {
		FBmap[FBrequests[i].Username] = i
	}

	//Setting up the new Consumer
	config = nsq.NewConfig()
	q, _ = nsq.NewConsumer("twitter", "ch", config)

	///Creating the Facebook Messages
	var TWrequests []NSQTwitterMessage = getTwitterMessages(*wg, c, *config, *q)

	//Initializing the FB Map
	TWmap := make(map[string]int)

	///Setting up the TW Screen Name Map
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

	// we initialize our influencer array
	var influencer []Influencer

	// Unmarshaling our byteArray which contains our
	// jsonFile's content into 'influencer' which we defined above
	json.Unmarshal([]byte(byteValue), &influencer)

	///Calling a function to look through the FB Messages
	ProcessFBmessage(influencer, FBrequests, TWrequests, TWmap)

	///Calling a function to look through the TW Messages
	ProcessTWmessage(influencer, TWrequests, FBrequests, FBmap)

}
