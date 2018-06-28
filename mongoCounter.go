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
	Host                  string
	DB                    string
	Collection            string
	PersistenceInterval   time.Duration
	stopped               bool
	mux                   sync.Mutex
	backgroundPersistance chan bool
}

func (m *MongoCounter) StartBackgroundPersistance() {
	if m.stopped {
		log.Printf("MongoCounter - Starting persistance each %v\n", m.PersistenceInterval)
		m.stopped = false
		go func(backPersist chan bool, interval time.Duration) {
			nextPersistance := time.Now().Add(interval)
			for {
				select {
				case <-backPersist:
					return
				default:
					if time.Now().After(nextPersistance) {
						m.Persist()
						nextPersistance = time.Now().Add(interval)
					}
				}
			}
		}(m.backgroundPersistance, m.PersistenceInterval)
	}
}

func (m *MongoCounter) Stop() {
	m.stopped = true
	m.backgroundPersistance <- true
	m.Persist()
}

func (m *MongoCounter) Clear(key string) {
	session, err := mgo.Dial(m.Host)
	if err != nil {
		log.Println(err)
		return
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	removeCollectionVal(session, m.DB, m.Collection, key)
	m.Counter.Clear(key)
}

func (m *MongoCounter) Persist() {
	session, err := mgo.Dial(m.Host)
	if err != nil {
		log.Println(err)
		return
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	m.mux.Lock()
	defer m.mux.Unlock()
	m.Range(func(k, v interface{}) bool {
		log.Printf("Persisting key: %s...\n", k)
		err := persistCollectionVal(session, m.DB, m.Collection, k.(string), v)
		if err != nil {
			log.Println(err)
		}
		return true
	})
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
