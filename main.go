package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/boltdb/bolt"
)

type DataPoint struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func InsertDataPoint(db *bolt.DB, dataPoint DataPoint) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		key := itob(dataPoint.Timestamp)
		value, err := json.Marshal(dataPoint)
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})
}

func QueryData(db *bolt.DB, start, end int64) ([]DataPoint, error) {
	var dataPoints []DataPoint
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return fmt.Errorf("Bucket data not found")
		}
		c := b.Cursor()
		min := itob(start)
		max := itob(end)
		for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
			var dataPoint DataPoint
			if err := json.Unmarshal(v, &dataPoint); err != nil {
				return err
			}
			dataPoints = append(dataPoints, dataPoint)
		}
		return nil
	})
	return dataPoints, err
}

func main() {
	db, err := bolt.Open("mydb.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	dataPoint := DataPoint{Timestamp: time.Now().Unix(), Value: 3.14}
	err = InsertDataPoint(db, dataPoint)
	if err != nil {
		log.Fatal(err)
	}

	dataPoints, err := QueryData(db, time.Now().Add(-time.Hour).Unix(), time.Now().Add(time.Hour).Unix())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dataPoints)
}
