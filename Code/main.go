package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
	Id       int    `json:"id"`
	Username string `json:"screen_name"`
	Bio      string `json:"bio"`
	UpdateAt string `json:"updated_at"`
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

func main() {

	wg := &sync.WaitGroup{}
	wg.Add(2)
	c := make(chan struct{})

	config := nsq.NewConfig()
	q, _ := nsq.NewConsumer("facebook", "ch", config)

	///Creating the Facebook Messages
	var FBrequests []NSQFacebookMessage = getFBMessages(*wg, c, *config, *q)

	// for i := 0; i < len(FBrequests); i++ {
	// 	fmt.Println("Facebook Message: " + FBrequests[i].Username)
	// }

	///Setting up the new Consumer
	config = nsq.NewConfig()
	q, _ = nsq.NewConsumer("twitter", "ch", config)

	///Creating the Facebook Messages
	var TWrequests []NSQTwitterMessage = getTwitterMessages(*wg, c, *config, *q)

	for i := 0; i < len(TWrequests); i++ {
		//fmt.Println("Twitter Message: " + TWrequests[i].Screen_name)
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

	for i := 0; i < len(FBrequests); i++ {
		found := false
		for x := 0; x < len(influencer); x++ {

			if FBrequests[i].Id == influencer[x].Source.Profile.Facebook.Id {
				found = true

				//Get Date values
				UpdateAtString := []rune(FBrequests[i].Updated_at)
				year := string(UpdateAtString[0:4])
				fmt.Println(" RUNE SUBSTRING:", year)

				break
			}
		}

		if found == false {
			//fmt.Println("Did not find: " + strconv.Itoa(FBrequests[i].Id))
		}
	}

	// // we iterate through every user within our users array and
	// // print out the user Type, their name, and their facebook url
	// // as just an example
	// for i := 0; i < len(influencer); i++ {

	// 	if influencer[i].Source.Profile.Facebook.Id != 0 {
	// 		var found bool = false
	// 		for x := 0; x < len(FBrequests); x++ {

	// 		}

	// 	}

	// 	if influencer[i].Source.Profile.Twitter.Id != 0 {

	// 	}

	// 	fmt.Println("Influencers Type: " + influencer[i].Id)
	// 	// fmt.Println("User Age: " + strconv.Itoa(users.Users[i].Age))
	// 	// fmt.Println("User Name: " + users.Users[i].Name)
	// 	// fmt.Println("Facebook Url: " + users.Users[i].Social.Facebook)
	// 	// fmt.Println("Id: " + strconv.Itoa(users.Users[i].Id))
	// }
}
