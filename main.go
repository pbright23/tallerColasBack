package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"

	"github.com/go-redis/redis"
)

func getRedi() string {
	// Connect to Redis server
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Retrieve the string from Redis
	val, err := client.Get("msg").Result()
	if err != nil {
		panic(err)
	}
	return(val)
}

func iniRedi() {
	// Connect to Redis server
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Save a string in Redis
	err := client.Set("msg", "¡Gracias por tu apoyo!", 0).Err()
	if err != nil {
		panic(err)
	}

}

func main() {

	iniRedi()
	
	//redisMSG := getRedi()
	//fmt.Printf(redisMSG)
	
	// Set up a Kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	
	// Create a Kafka producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()
	
	// Set up a Kafka consumer config
	config = sarama.NewConfig()
	config.Consumer.Return.Errors = true
	
	// Create a Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
		}()
		
	// Set up a Kafka topic to send and receive messages
	topic1 := "test" //recibe el mensage
	topic2 := "dos"  //envía el mensage
	
	// Consume messages from Kafka
	partitions, err := consumer.Partitions(topic1)
	if err != nil {
		panic(err)
	}
	
	for _, partition := range partitions {
		// Start a Kafka partition consumer
		pc, err := consumer.ConsumePartition(topic1, partition, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := pc.Close(); err != nil {
				panic(err)
			}
		}()

		// Start a goroutine to consume messages from the Kafka partition
		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Message received from partition %d at offset %d: %s\n", msg.Partition, msg.Offset, string(msg.Value))

				// Send a message to Kafka
				msg := &sarama.ProducerMessage{
					Topic: topic2,
					Value: sarama.StringEncoder(getRedi()),
				}
				partition, offset, err := producer.SendMessage(msg)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)

			}
		}(pc)
	}


	// Wait for a signal to exit the program
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
}