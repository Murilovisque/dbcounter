package dbcounter_test

import (
	"log"
	"testing"
	"time"

	"github.com/Murilovisque/dbcounter"
	mgo "gopkg.in/mgo.v2"
)

const (
	qtdeTest         = 10000
	qtdeDurationTest = time.Duration(qtdeTest)
	hostTest         = "localhost"
	dbTest           = "counter-test-db"
	collectionTest   = "counterstest"
)

func TestIncAndPersistAndVal(t *testing.T) {
	dropDataBase(dbTest)
	m := dbcounter.MongoCounter{Host: hostTest, DB: dbTest, Collection: collectionTest}
	m.Inc("k1d", qtdeDurationTest)
	m.Inc("k2d", qtdeDurationTest)
	m.Inc("k1i", qtdeTest)
	m.Inc("k2i", qtdeTest)
	m.Persist()
	passIfAreEqualsDurationWhenUseVal(t, qtdeDurationTest, &m, "k1d", "k2d")
	passIfAreEqualsIntWhenUseVal(t, qtdeTest, &m, "k1i", "k2i")
}

func TestIncAndUpdateFromDBInOtherInstance(t *testing.T) {
	dropDataBase(dbTest)
	m := dbcounter.MongoCounter{Host: hostTest, DB: dbTest, Collection: collectionTest}
	m.Inc("k1d", qtdeDurationTest)
	m.Inc("k1i", qtdeTest)
	m.Persist()
	m = dbcounter.MongoCounter{Host: hostTest, DB: dbTest, Collection: collectionTest}
	err := m.UpdateFromDB()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	passIfAreEqualsDurationWhenUseVal(t, qtdeDurationTest, &m, "k1d")
	passIfAreEqualsIntWhenUseVal(t, qtdeTest, &m, "k1i")
}

func TestUpdateWithoutDB(t *testing.T) {
	dropDataBase(dbTest)
	m := dbcounter.MongoCounter{Host: hostTest, DB: dbTest, Collection: collectionTest}
	err := m.UpdateFromDB()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func passIfAreEqualsDurationWhenUseVal(t *testing.T, assertVal time.Duration, c *dbcounter.MongoCounter, keys ...string) {
	comp := func(a, b interface{}) bool {
		v1, ok := a.(time.Duration)
		if !ok {
			return false
		}
		v2, ok := a.(time.Duration)
		return ok && v1 == v2
	}
	passIfAreEquals(comp, t, assertVal, c, keys...)
}

func passIfAreEqualsIntWhenUseVal(t *testing.T, assertVal int, c *dbcounter.MongoCounter, keys ...string) {
	comp := func(a, b interface{}) bool {
		v1, ok := a.(int)
		if !ok {
			return false
		}
		v2, ok := a.(int)
		return ok && v1 == v2
	}
	passIfAreEquals(comp, t, assertVal, c, keys...)
}

func passIfAreEquals(comparator func(interface{}, interface{}) bool, t *testing.T, assertVal interface{}, c *dbcounter.MongoCounter, keys ...string) {
	if t.Failed() {
		return
	}
	for _, k := range keys {
		v, ok := c.Val(k)
		if !ok || !comparator(v, assertVal) {
			log.Printf("Test %s failed, value of key '%s' should be %v, but it is %v\n", t.Name(), k, assertVal, v)
			t.FailNow()
			break
		}
	}
}

func dropDataBase(db string) {
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	session.DB(db).DropDatabase()
}
