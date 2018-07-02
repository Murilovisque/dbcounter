package dbcounter

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Murilovisque/counter"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	keyField = "key"
	valField = "val"
)

type MongoCounter struct {
	counter.Counter
	Host                              string
	DB                                string
	Collection                        string
	PersistenceInterval               time.Duration
	backgroundPersistanceRunning      bool
	mux                               sync.Mutex
	backgroundPersistanceStopNotifier chan bool
}

func (m *MongoCounter) StartBackgroundPersistance() {
	if !m.backgroundPersistanceRunning {
		log.Printf("MongoCounter - Starting background persistance each %v\n", m.PersistenceInterval)
		m.backgroundPersistanceRunning = true
		m.backgroundPersistanceStopNotifier = make(chan bool)
		if m.PersistenceInterval == 0 {
			log.Println("MongoCounter - PersistenceInterval should not be zero, use default value 10 seconds")
			m.PersistenceInterval = 10 * time.Second
		}
		go func(mongo *MongoCounter) {
			tick := time.Tick(mongo.PersistenceInterval)
			for {
				select {
				case <-mongo.backgroundPersistanceStopNotifier:
					return
				case <-tick:
					err := mongo.Persist()
					if err == nil {
						log.Println("MongoCounter - Background persistance executed")
					} else {
						log.Printf("MongoCounter - Error in background persistance: %v\n", err)
					}
				}
			}
		}(m)
		log.Println("MongoCounter - Background persistance started")
	}
}

func (m *MongoCounter) Stop() {
	log.Println("MongoCounter - Stopping background persistance")
	m.backgroundPersistanceRunning = false
	m.backgroundPersistanceStopNotifier <- true
	m.Persist()
	log.Println("MongoCounter - Background persistance stopped")
}

func (m *MongoCounter) Clear(key string) error {
	session, err := mgo.Dial(m.Host)
	if err != nil {
		return err
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	err = removeCollectionVal(session, m.DB, m.Collection, key)
	if err != nil {
		return err
	}
	m.Counter.Clear(key)
	return nil
}

func (m *MongoCounter) Persist() error {
	session, err := mgo.Dial(m.Host)
	if err != nil {
		return err
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	m.mux.Lock()
	defer m.mux.Unlock()
	m.Range(func(k, v interface{}) bool {
		log.Printf("MongoCounter - Persisting key: %s...\n", k)
		err = persistCollectionVal(session, m.DB, m.Collection, k.(string), v)
		if err != nil {
			return false
		}
		log.Printf("MongoCounter - Key persisted: %s...\n", k)
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoCounter) UpdateFromDB() error {
	session, err := mgo.Dial(m.Host)
	if err != nil {
		return err
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	collection := session.DB(m.DB).C(m.Collection)
	var list []countMongo
	err = collection.Find(bson.M{}).All(&list)
	if err != nil {
		return err
	}
	for _, c := range list {
		m.Inc(c.Key, c.ValConverted())
	}
	return nil
}

func persistCollectionVal(session *mgo.Session, db, collectionName, key string, val interface{}) error {
	collection := session.DB(db).C(collectionName)
	var id bson.ObjectId
	if err := collection.Find(bson.M{keyField: key}).Select(bson.M{"_id": 1}).One(&id); err == mgo.ErrNotFound {
		return collection.Insert(&countMongo{Key: key, Val: val, ValType: fmt.Sprintf("%T", val)})
	}
	return collection.Update(bson.M{"_id": id}, bson.M{"$set": bson.M{valField: val}})
}

func removeCollectionVal(session *mgo.Session, db, collectionName, key string) error {
	collection := session.DB(db).C(collectionName)
	return collection.Remove(bson.M{keyField: key})
}

type countMongo struct {
	ID      bson.ObjectId `bson:"_id,omitempty"`
	Key     string
	Val     interface{}
	ValType string
}

func (c *countMongo) ValConverted() interface{} {
	switch {
	case c.ValType == "time.Duration":
		return time.Duration(c.Val.(int64))
	default:
		return c.Val
	}
}
