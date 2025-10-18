package encoding

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"lsmdb/pkg/encoding/custom"
	pb "lsmdb/pkg/proto"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
)

// Tweet представляет структуру данных из tweets.csv
type Tweet struct {
	ID        string
	User      string
	Fullname  string
	URL       string
	Timestamp time.Time
	Replies   int
	Likes     int
	Retweets  int
	Text      string
}

func loadTweets(count int) ([]Tweet, error) {
	tweetsFile, err := os.Open("/home/andrew/Documents/lsmdb/archive (2)/tweets.csv")
	if err != nil {
		return nil, err
	}
	defer tweetsFile.Close()

	tweetReader := csv.NewReader(tweetsFile)
	tweetReader.Comma = ';'
	// Пропускаем заголовок
	_, err = tweetReader.Read()
	if err != nil {
		return nil, err
	}

	tweets := make([]Tweet, 0, count)
	for i := 0; i < count; i++ {
		record, err := tweetReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		timestamp, _ := time.Parse("2006-01-02 15:04:05-07", strings.TrimSpace(record[4]))
		replies, _ := strconv.Atoi(record[5])
		likes, _ := strconv.Atoi(record[6])
		retweets, _ := strconv.Atoi(record[7])

		tweets = append(tweets, Tweet{
			ID:        record[0],
			User:      record[1],
			Fullname:  record[2],
			URL:       record[3],
			Timestamp: timestamp,
			Replies:   replies,
			Likes:     likes,
			Retweets:  retweets,
			Text:      record[8],
		})
	}
	return tweets, nil
}

func BenchmarkBulkEncoding(b *testing.B) {
	sizes := []int{50000, 75000, 100000, 250000, 500000, 750000, 1000000, 2500000, 5000000}

	// Создаем кодек AVRO
	tweetCodec, err := goavro.NewCodec(`{
		"type": "record",
		"name": "Tweet",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "user", "type": "string"},
			{"name": "fullname", "type": "string"},
			{"name": "url", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "replies", "type": "int"},
			{"name": "likes", "type": "int"},
			{"name": "retweets", "type": "int"},
			{"name": "text", "type": "string"}
		]
	}`)
	if err != nil {
		b.Fatal(err)
	}

	for _, size := range sizes {
		// Загружаем данные
		tweets, err := loadTweets(size)
		if err != nil {
			b.Fatal(err)
		}
		if len(tweets) == 0 {
			b.Fatal("No tweets loaded")
		}

		// Тесты для твитов
		b.Run(fmt.Sprintf("Tweets-%d-Custom", size), func(b *testing.B) {
			// Прогрев
			for i := 0; i < 1000; i++ {
				tweet := tweets[i%len(tweets)]
				msg := custom.Value{
					Type: custom.TypeMessage,
					Message: []custom.Field{
						{Number: 1, Value: custom.Value{Type: custom.TypeString, String: tweet.ID}},
						{Number: 2, Value: custom.Value{Type: custom.TypeString, String: tweet.User}},
						{Number: 3, Value: custom.Value{Type: custom.TypeString, String: tweet.Fullname}},
						{Number: 4, Value: custom.Value{Type: custom.TypeString, String: tweet.URL}},
						{Number: 5, Value: custom.Value{Type: custom.TypeInt64, Int64: tweet.Timestamp.UnixNano()}},
						{Number: 6, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Replies)}},
						{Number: 7, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Likes)}},
						{Number: 8, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Retweets)}},
						{Number: 9, Value: custom.Value{Type: custom.TypeString, String: tweet.Text}},
					},
				}
				encoded, _ := custom.Encode(msg)
				decoded, _, _ := custom.Decode(encoded)
				// Проверяем корректность декодирования
				if decoded.Type != custom.TypeMessage ||
					len(decoded.Message) != 9 ||
					decoded.Message[0].Value.String != tweet.ID ||
					decoded.Message[1].Value.String != tweet.User {
					b.Fatal("Custom decode verification failed")
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, tweet := range tweets {
					msg := custom.Value{
						Type: custom.TypeMessage,
						Message: []custom.Field{
							{Number: 1, Value: custom.Value{Type: custom.TypeString, String: tweet.ID}},
							{Number: 2, Value: custom.Value{Type: custom.TypeString, String: tweet.User}},
							{Number: 3, Value: custom.Value{Type: custom.TypeString, String: tweet.Fullname}},
							{Number: 4, Value: custom.Value{Type: custom.TypeString, String: tweet.URL}},
							{Number: 5, Value: custom.Value{Type: custom.TypeInt64, Int64: tweet.Timestamp.UnixNano()}},
							{Number: 6, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Replies)}},
							{Number: 7, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Likes)}},
							{Number: 8, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Retweets)}},
							{Number: 9, Value: custom.Value{Type: custom.TypeString, String: tweet.Text}},
						},
					}
					encoded, _ := custom.Encode(msg)
					decoded, _, _ := custom.Decode(encoded)
					// Проверяем корректность декодирования
					if decoded.Type != custom.TypeMessage ||
						len(decoded.Message) != 9 ||
						decoded.Message[0].Value.String != tweet.ID ||
						decoded.Message[1].Value.String != tweet.User {
						b.Fatal("Custom decode verification failed")
					}
				}
			}
		})

		b.Run(fmt.Sprintf("Tweets-%d-Protobuf", size), func(b *testing.B) {
			// Прогрев
			for i := 0; i < 1000; i++ {
				tweet := tweets[i%len(tweets)]
				pbMsg := &pb.SocialPost{
					Id:        tweet.ID,
					UserId:    tweet.User,
					Fullname:  tweet.Fullname,
					Url:       tweet.URL,
					Timestamp: tweet.Timestamp.UnixNano(),
					Replies:   int32(tweet.Replies),
					Likes:     int32(tweet.Likes),
					Retweets:  int32(tweet.Retweets),
					Text:      tweet.Text,
				}
				encoded, _ := proto.Marshal(pbMsg)
				decoded := &pb.SocialPost{}
				_ = proto.Unmarshal(encoded, decoded)
				// Проверяем корректность декодирования
				if decoded.Id != tweet.ID ||
					decoded.UserId != tweet.User ||
					decoded.Fullname != tweet.Fullname {
					b.Fatal("Protobuf decode verification failed")
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, tweet := range tweets {
					pbMsg := &pb.SocialPost{
						Id:        tweet.ID,
						UserId:    tweet.User,
						Fullname:  tweet.Fullname,
						Url:       tweet.URL,
						Timestamp: tweet.Timestamp.UnixNano(),
						Replies:   int32(tweet.Replies),
						Likes:     int32(tweet.Likes),
						Retweets:  int32(tweet.Retweets),
						Text:      tweet.Text,
					}
					encoded, _ := proto.Marshal(pbMsg)
					decoded := &pb.SocialPost{}
					_ = proto.Unmarshal(encoded, decoded)
					// Проверяем корректность декодирования
					if decoded.Id != tweet.ID ||
						decoded.UserId != tweet.User ||
						decoded.Fullname != tweet.Fullname {
						b.Fatal("Protobuf decode verification failed")
					}
				}
			}
		})

		b.Run(fmt.Sprintf("Tweets-%d-Avro", size), func(b *testing.B) {
			// Прогрев
			for i := 0; i < 1000; i++ {
				tweet := tweets[i%len(tweets)]
				native := map[string]interface{}{
					"id":        tweet.ID,
					"user":      tweet.User,
					"fullname":  tweet.Fullname,
					"url":       tweet.URL,
					"timestamp": tweet.Timestamp.UnixNano(),
					"replies":   int32(tweet.Replies),
					"likes":     int32(tweet.Likes),
					"retweets":  int32(tweet.Retweets),
					"text":      tweet.Text,
				}
				encoded, _ := tweetCodec.BinaryFromNative(nil, native)
				decoded, _, _ := tweetCodec.NativeFromBinary(encoded)
				// Проверяем корректность декодирования
				if m, ok := decoded.(map[string]interface{}); ok {
					if m["id"] != tweet.ID ||
						m["user"] != tweet.User ||
						m["fullname"] != tweet.Fullname {
						b.Fatal("Avro decode verification failed")
					}
				} else {
					b.Fatal("Avro decode returned wrong type")
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, tweet := range tweets {
					native := map[string]interface{}{
						"id":        tweet.ID,
						"user":      tweet.User,
						"fullname":  tweet.Fullname,
						"url":       tweet.URL,
						"timestamp": tweet.Timestamp.UnixNano(),
						"replies":   int32(tweet.Replies),
						"likes":     int32(tweet.Likes),
						"retweets":  int32(tweet.Retweets),
						"text":      tweet.Text,
					}
					encoded, _ := tweetCodec.BinaryFromNative(nil, native)
					decoded, _, _ := tweetCodec.NativeFromBinary(encoded)
					// Проверяем корректность декодирования
					if m, ok := decoded.(map[string]interface{}); ok {
						if m["id"] != tweet.ID ||
							m["user"] != tweet.User ||
							m["fullname"] != tweet.Fullname {
							b.Fatal("Avro decode verification failed")
						}
					} else {
						b.Fatal("Avro decode returned wrong type")
					}
				}
			}
		})

		// Выводим размеры для первой записи каждого типа
		tweet := tweets[0]
		customMsg := custom.Value{
			Type: custom.TypeMessage,
			Message: []custom.Field{
				{Number: 1, Value: custom.Value{Type: custom.TypeString, String: tweet.ID}},
				{Number: 2, Value: custom.Value{Type: custom.TypeString, String: tweet.User}},
				{Number: 3, Value: custom.Value{Type: custom.TypeString, String: tweet.Fullname}},
				{Number: 4, Value: custom.Value{Type: custom.TypeString, String: tweet.URL}},
				{Number: 5, Value: custom.Value{Type: custom.TypeInt64, Int64: tweet.Timestamp.UnixNano()}},
				{Number: 6, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Replies)}},
				{Number: 7, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Likes)}},
				{Number: 8, Value: custom.Value{Type: custom.TypeInt32, Int32: int32(tweet.Retweets)}},
				{Number: 9, Value: custom.Value{Type: custom.TypeString, String: tweet.Text}},
			},
		}
		customData, _ := custom.Encode(customMsg)

		pbMsg := &pb.SocialPost{
			Id:        tweet.ID,
			UserId:    tweet.User,
			Fullname:  tweet.Fullname,
			Url:       tweet.URL,
			Timestamp: tweet.Timestamp.UnixNano(),
			Replies:   int32(tweet.Replies),
			Likes:     int32(tweet.Likes),
			Retweets:  int32(tweet.Retweets),
			Text:      tweet.Text,
		}
		protoData, _ := proto.Marshal(pbMsg)

		avroNative := map[string]interface{}{
			"id":        tweet.ID,
			"user":      tweet.User,
			"fullname":  tweet.Fullname,
			"url":       tweet.URL,
			"timestamp": tweet.Timestamp.UnixNano(),
			"replies":   int32(tweet.Replies),
			"likes":     int32(tweet.Likes),
			"retweets":  int32(tweet.Retweets),
			"text":      tweet.Text,
		}
		avroData, _ := tweetCodec.BinaryFromNative(nil, avroNative)

		b.Logf("\nEncoded sizes for %d records:", size)
		b.Logf("Custom: %d bytes per record", len(customData))
		b.Logf("Protobuf: %d bytes per record", len(protoData))
		b.Logf("Avro: %d bytes per record", len(avroData))
	}
}
